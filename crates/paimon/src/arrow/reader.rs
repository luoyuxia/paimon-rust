use crate::deletion_vector::DeletionVector;
use crate::io::{FileIO, FileRead, FileStatus};
use crate::scan::{ArrowRecordBatchStream, FileScanTaskStream};
use crate::spec::TableSchemaRef;
use arrow::array::RecordBatch;
use async_stream::try_stream;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{StreamExt, TryFutureExt};
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::ProjectionMask;
use parquet::file::metadata::ParquetMetaData;

use arrow_array::UInt64Array;
use parquet::file::metadata::ParquetMetaDataReader;
use std::ops::Range;
use std::sync::Arc;
use tokio::try_join;

pub struct ArrowReaderBuilder {
    batch_size: Option<usize>,
    file_io: FileIO,
    table_schema: TableSchemaRef,
}

impl ArrowReaderBuilder {
    /// Create a new ArrowReaderBuilder
    pub(crate) fn new(file_io: FileIO, table_schema: TableSchemaRef) -> Self {
        ArrowReaderBuilder {
            batch_size: None,
            file_io,
            table_schema,
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
            table_schema: self.table_schema,
        }
    }
}

/// Reads data from Parquet files
#[derive(Clone)]
pub struct ArrowReader {
    batch_size: Option<usize>,
    file_io: FileIO,
    table_schema: TableSchemaRef,
}

impl ArrowReader {
    /// Take a stream of FileScanTasks and reads all the files.
    /// Returns a stream of Arrow RecordBatches containing the data from the files
    pub fn read(self, mut tasks: FileScanTaskStream) -> crate::Result<ArrowRecordBatchStream> {
        let file_io = self.file_io.clone();
        let batch_size = self.batch_size;
        let table_schema = self.table_schema.clone();

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

                        // Build projection based on table_schema if available
                        let parquet_schema = batch_stream_builder.parquet_schema();
                        let projection =  Self::build_projection_from_table_schema(parquet_schema,
                           &self.table_schema);
                        if let Some(proj) = projection {
                            batch_stream_builder = batch_stream_builder.with_projection(proj);
                        }

                        if let Some(size) = batch_size {
                            batch_stream_builder = batch_stream_builder.with_batch_size(size);
                        }

                        let mut batch_stream = batch_stream_builder.build()?;
                        let deletion_vector = task.deletion_vector().cloned();
                        let mut row_offset: u64 = 0;

                        while let Some(batch) = batch_stream.next().await {
                            let batch = batch?;
                            // Apply deletion vector if available
                            let filtered_batch = if let Some(dv) = &deletion_vector {
                                Self::filter_deleted_rows(batch, dv, row_offset)?
                            } else {
                                batch
                            };
                            row_offset += filtered_batch.num_rows() as u64;
                            yield filtered_batch;
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
        Box::pin(self.r.read(range.start..range.end).map_err(|err| {
            let err_msg = format!("{}", err);
            parquet::errors::ParquetError::External(err_msg.into())
        }))
    }

    fn get_metadata(
        &mut self,
        options: Option<&ArrowReaderOptions>,
    ) -> BoxFuture<parquet::errors::Result<Arc<ParquetMetaData>>> {
        Box::pin(async move {
            let file_size = self.meta.size;
            Ok(Arc::new(
                ParquetMetaDataReader::new()
                    .load_and_finish(self, file_size)
                    .await?,
            ))
        })
    }
}

impl ArrowReader {
    /// Filter deleted rows from a RecordBatch using a DeletionVector
    fn filter_deleted_rows(
        batch: RecordBatch,
        deletion_vector: &Arc<DeletionVector>,
        row_offset: u64,
    ) -> crate::Result<RecordBatch> {
        let num_rows = batch.num_rows();
        let mut keep_indices = Vec::with_capacity(num_rows);

        // Build a list of row indices to keep (those not in deletion vector)
        for i in 0..num_rows {
            let global_row_pos = row_offset + i as u64;
            if !deletion_vector.is_deleted(global_row_pos) {
                keep_indices.push(i as u64);
            }
        }

        // If all rows are kept, return the original batch
        if keep_indices.len() == num_rows {
            return Ok(batch);
        }

        // If no rows are kept, return an empty batch with the same schema
        if keep_indices.is_empty() {
            return Ok(RecordBatch::new_empty(batch.schema()));
        }

        // Filter the batch using the keep indices
        let indices = UInt64Array::from(keep_indices);

        // Apply take operation to each column
        let mut filtered_columns = Vec::with_capacity(batch.num_columns());
        for col_idx in 0..batch.num_columns() {
            let column = batch.column(col_idx);
            let filtered_column = arrow::compute::take(column, &indices, None).map_err(|e| {
                crate::Error::DataInvalid {
                    message: format!("Failed to filter column {}: {}", col_idx, e),
                    source: Some(Box::new(e)),
                }
            })?;
            filtered_columns.push(filtered_column);
        }

        let filtered_batch =
            RecordBatch::try_new(batch.schema(), filtered_columns).map_err(|e| {
                crate::Error::DataInvalid {
                    message: format!("Failed to create filtered batch: {}", e),
                    source: Some(Box::new(e)),
                }
            })?;

        Ok(filtered_batch)
    }

    /// Build a projection based on table_schema
    ///
    /// Only reads columns that are defined in the table_schema, which automatically
    /// excludes internal columns like `_KEY_*`, `_SEQUENCE_NUMBER`, and `_VALUE_KIND`.
    fn build_projection_from_table_schema(
        parquet_schema: &parquet::schema::types::SchemaDescriptor,
        table_schema: &TableSchemaRef,
    ) -> Option<ProjectionMask> {
        // Get field names from table_schema
        let table_field_names: std::collections::HashSet<&str> = table_schema
            .fields()
            .iter()
            .map(|f| f.name().as_ref())
            .collect();

        // Get all field names from parquet schema
        let root_group = parquet_schema.root_schema();
        let mut column_names: Vec<&str> = Vec::new();

        // Only include columns that exist in both parquet schema and table schema
        for field in root_group.get_fields() {
            let field_name = field.name();
            if table_field_names.contains(field_name) {
                column_names.push(field_name);
            }
        }

        // If no matching columns found, return empty projection
        if column_names.is_empty() {
            return Some(ProjectionMask::columns(parquet_schema, Vec::<&str>::new()));
        }

        // If all parquet columns are in table schema, return None to read all
        // (This is unlikely, but handle it for completeness)
        let all_parquet_fields: Vec<&str> =
            root_group.get_fields().iter().map(|f| f.name()).collect();
        if column_names.len() == all_parquet_fields.len()
            && column_names
                .iter()
                .all(|name| all_parquet_fields.contains(name))
        {
            return None;
        }

        // Create projection mask with only the columns in table schema
        Some(ProjectionMask::columns(parquet_schema, column_names))
    }
}
