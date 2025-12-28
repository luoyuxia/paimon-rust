use crate::io::{FileIO, FileRead, FileStatus};
use crate::scan::{ArrowRecordBatchStream, FileScanTaskStream};
use async_stream::try_stream;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{StreamExt, TryFutureExt};
use parquet::arrow::async_reader::{AsyncFileReader, MetadataLoader};
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::file::metadata::ParquetMetaData;
use std::ops::Range;
use std::sync::Arc;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
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
    /// Take a stream of FileScanTasks and reads all the files.
    /// Returns a stream of Arrow RecordBatches containing the data from the files
    pub fn read(self, mut tasks: FileScanTaskStream) -> crate::Result<ArrowRecordBatchStream> {
        let file_io = self.file_io.clone();
        let batch_size = self.batch_size;

        Ok(try_stream! {
            while let Some(task_result) = tasks.next().await {
                match task_result {
                    Ok(task) => {
                        let parquet_file = file_io
                            .new_input(task.data_file_path())?;

                        let (parquet_metadata, parquet_reader) = try_join!(
                            parquet_file.metadata(),
                            parquet_file.reader()
                        )?;

                        let arrow_file_reader = ArrowFileReader::new(parquet_metadata, parquet_reader);

                        let mut batch_stream_builder = ParquetRecordBatchStreamBuilder::new(arrow_file_reader)
                            .await?;

                        if let Some(size) = batch_size {
                            batch_stream_builder = batch_stream_builder.with_batch_size(size);
                        }

                        let mut batch_stream = batch_stream_builder.build()?;

                        while let Some(batch) = batch_stream.next().await {
                            yield batch?;
                        }
                    }
                    Err(e) => {
                        Err(e)?;
                    }
                }
            }
        }.boxed())
    }
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
        Box::pin(
            self.r
                .read(range.start..range.end)
                .map_err(|err| {
                    let err_msg = format!("{}", err);
                    parquet::errors::ParquetError::External(err_msg.into())
                }),
        )
    }

    fn get_metadata(&mut self, options: Option<&ArrowReaderOptions>) -> BoxFuture<parquet::errors::Result<Arc<ParquetMetaData>>> {
        Box::pin(async move {
            let file_size = self.meta.size;
            let mut loader = MetadataLoader::load(self, file_size as usize, None).await?;
            loader.load_page_index(false, false).await?;
            Ok(Arc::new(loader.finish()))
        })
    }
}
