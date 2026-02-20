// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Arrow reader with optional deletion vector filtering.
//!
//! Reference: [RawFileSplitRead.createReader](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/operation/RawFileSplitRead.java).
//! Uses [DeletionVectorFactory] to resolve deletion vectors by file name (same as Java's DeletionVector.Factory).

use crate::deletion_vector::{DeletionVector, DeletionVectorFactory};
use crate::io::{FileIO, FileRead, FileStatus};
use crate::table::ArrowRecordBatchStream;
use crate::DataSplit;
use async_stream::try_stream;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{StreamExt, TryFutureExt};
use parquet::arrow::arrow_reader::{ArrowReaderOptions, RowSelection, RowSelector};
use parquet::arrow::async_reader::{AsyncFileReader, MetadataLoader};
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};
use std::ops::Range;
use std::sync::Arc;
use tokio::try_join;

pub struct ArrowReaderBuilder {
    batch_size: Option<usize>,
    file_io: FileIO,
}

impl ArrowReaderBuilder {
    /// Create a new ArrowReaderBuilder
    pub(crate) fn new(file_io: FileIO) -> Self {
        ArrowReaderBuilder {
            batch_size: None,
            file_io,
        }
    }

    /// Sets the desired size of batches in the response
    /// to something other than the default
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = Some(batch_size);
        self
    }

    /// Build the ArrowReader.
    pub fn build(self) -> ArrowReader {
        ArrowReader {
            batch_size: self.batch_size,
            file_io: self.file_io,
        }
    }
}

/// Reads data from Parquet files
#[derive(Clone)]
pub struct ArrowReader {
    batch_size: Option<usize>,
    file_io: FileIO,
}

impl ArrowReader {
    /// Take a stream of DataSplits and read every data file in each split.
    /// When a split has deletion files (see [DataSplit::data_deletion_files]), the corresponding
    /// deletion vectors are loaded and applied so that deleted rows are filtered out from the stream.
    /// Row positions are 0-based within each data file, matching Java's ApplyDeletionVectorReader.
    ///
    /// Matches [RawFileSplitRead.createReader](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/operation/RawFileSplitRead.java):
    /// one DV factory per DataSplit (created from that split's data files and deletion files).
    pub fn read(self, data_splits: &[DataSplit]) -> crate::Result<ArrowRecordBatchStream> {
        let file_io = self.file_io.clone();
        let batch_size = self.batch_size;
        // Owned list of splits so the stream does not hold references.
        let splits: Vec<DataSplit> = data_splits.to_vec();

        Ok(try_stream! {
            for split in splits {
                // Create DV factory for this split only (like Java createReader(partition, bucket, files, deletionFiles)).
                // Build (file_name, opt_deletion_file) by index: data_deletion_files[i] corresponds to data_files[i].
                let dv_entries: Vec<(String, Option<crate::DeletionFile>)> = split
                    .data_files()
                    .iter()
                    .enumerate()
                    .map(|(i, f)| (f.file_name.clone(), split.deletion_file_for_data_file_index(i).cloned()))
                    .collect();
                let dv_factory = DeletionVectorFactory::create_from_deletion_files(
                    file_io.clone(),
                    dv_entries,
                )
                .await?;

                for (path_to_read, file_meta) in split.data_file_entries() {
                    let dv = dv_factory.get_deletion_vector(&file_meta.file_name);

                    let parquet_file = file_io.new_input(&path_to_read)?;
                    let (parquet_metadata, parquet_reader) = try_join!(
                        parquet_file.metadata(),
                        parquet_file.reader()
                    )?;
                    let arrow_file_reader = ArrowFileReader::new(parquet_metadata, parquet_reader);

                    let mut batch_stream_builder =
                        ParquetRecordBatchStreamBuilder::new(arrow_file_reader)
                            .await?;

                    if let Some(ref dv) = dv {
                        if !dv.is_empty() {
                            let row_selection =
                                build_deletes_row_selection(batch_stream_builder.metadata().row_groups(), dv)?;
                            batch_stream_builder = batch_stream_builder.with_row_selection(row_selection);
                        }
                    }
                    if let Some(size) = batch_size {
                        batch_stream_builder = batch_stream_builder.with_batch_size(size);
                    }
                    let mut batch_stream = batch_stream_builder.build()?;

                    while let Some(batch) = batch_stream.next().await {
                        let batch = batch?;
                        if batch.num_rows() > 0 {
                            yield batch;
                        }
                    }
                }
            }
        }
        .boxed())
    }
}

/// Builds a Parquet [RowSelection] from deletion vector.
/// Only rows not in the deletion vector are selected; deleted rows are skipped at read time.
/// Uses [DeletionVectorIterator] with [advance_to](DeletionVectorIterator::advance_to) when skipping row groups.
fn build_deletes_row_selection(
    row_group_metadata_list: &[RowGroupMetaData],
    deletion_vector: &DeletionVector,
) -> crate::Result<RowSelection> {
    let mut delete_iter = deletion_vector.iter();

    let mut results: Vec<RowSelector> = Vec::new();
    let mut current_row_group_base_idx: u64 = 0;
    let mut next_deleted_row_idx_opt = delete_iter.next();

    for row_group_metadata in row_group_metadata_list {
        let row_group_num_rows = row_group_metadata.num_rows() as u64;
        let next_row_group_base_idx = current_row_group_base_idx + row_group_num_rows;

        let mut next_deleted_row_idx = match next_deleted_row_idx_opt {
            Some(next_deleted_row_idx) => {
                if next_deleted_row_idx >= next_row_group_base_idx {
                    results.push(RowSelector::select(row_group_num_rows as usize));
                    current_row_group_base_idx += row_group_num_rows;
                    continue;
                }
                next_deleted_row_idx
            }
            None => {
                results.push(RowSelector::select(row_group_num_rows as usize));
                current_row_group_base_idx += row_group_num_rows;
                continue;
            }
        };

        let mut current_idx = current_row_group_base_idx;
        'chunks: while next_deleted_row_idx < next_row_group_base_idx {
            if current_idx < next_deleted_row_idx {
                let run_length = next_deleted_row_idx - current_idx;
                results.push(RowSelector::select(run_length as usize));
                current_idx += run_length;
            }
            let mut run_length = 0u64;
            while next_deleted_row_idx == current_idx
                && next_deleted_row_idx < next_row_group_base_idx
            {
                run_length += 1;
                current_idx += 1;
                next_deleted_row_idx_opt = delete_iter.next();
                next_deleted_row_idx = match next_deleted_row_idx_opt {
                    Some(v) => v,
                    None => {
                        results.push(RowSelector::skip(run_length as usize));
                        break 'chunks;
                    }
                };
            }
            if run_length > 0 {
                results.push(RowSelector::skip(run_length as usize));
            }
        }
        if current_idx < next_row_group_base_idx {
            results.push(RowSelector::select(
                (next_row_group_base_idx - current_idx) as usize,
            ));
        }
        current_row_group_base_idx += row_group_num_rows;
    }

    Ok(results.into())
}

/// ArrowFileReader is a wrapper around a FileRead that impls parquets AsyncFileReader.
///
/// # TODO
///
/// [ParquetObjectReader](https://docs.rs/parquet/latest/src/parquet/arrow/async_reader/store.rs.html#64)
/// contains the following hints to speed up metadata loading, we can consider adding them to this struct:
///
/// - `metadata_size_hint`: Provide a hint as to the size of the parquet file's footer.
/// - `preload_column_index`: Load the Column Index  as part of [`Self::get_metadata`].
/// - `preload_offset_index`: Load the Offset Index as part of [`Self::get_metadata`].
struct ArrowFileReader<R: FileRead> {
    meta: FileStatus,
    r: R,
}

impl<R: FileRead> ArrowFileReader<R> {
    /// Create a new ArrowFileReader
    fn new(meta: FileStatus, r: R) -> Self {
        Self { meta, r }
    }
}

impl<R: FileRead> AsyncFileReader for ArrowFileReader<R> {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        Box::pin(self.r.read(range.start..range.end).map_err(|err| {
            let err_msg = format!("{err}");
            parquet::errors::ParquetError::External(err_msg.into())
        }))
    }

    fn get_metadata(
        &mut self,
        options: Option<&ArrowReaderOptions>,
    ) -> BoxFuture<parquet::errors::Result<Arc<ParquetMetaData>>> {
        Box::pin(async move {
            let file_size = self.meta.size;
            let mut loader = MetadataLoader::load(self, file_size as usize, None).await?;
            loader.load_page_index(false, false).await?;
            Ok(Arc::new(loader.finish()))
        })
    }
}
