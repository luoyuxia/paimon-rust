mod context;

use crate::arrow::ArrowReaderBuilder;
use crate::catalog::Table;
use crate::io::FileIO;
use crate::spec::{
    from_avro_bytes, FileKind, Manifest, ManifestFileMeta, SnapshotRef, TableSchemaRef,
};
use crate::Error;
use arrow_array::RecordBatch;
use futures::stream::{self, BoxStream};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;

/// A stream of arrow [`RecordBatch`]es.
pub type ArrowRecordBatchStream = BoxStream<'static, crate::Result<RecordBatch>>;
use crate::Result;

pub struct TableScanBuilder<'a> {
    table: &'a Table,
    // Defaults to none which means select all columns
    column_names: Option<Vec<String>>,
    snapshot_id: Option<i64>,
}

impl<'a> TableScanBuilder<'a> {
    pub(crate) fn new(table: &'a Table) -> Self {
        Self {
            table,
            column_names: None,
            snapshot_id: None,
        }
    }

    pub fn select_all(mut self) -> Self {
        self.column_names = None;
        self
    }

    /// Select some columns of the table.
    pub fn select(mut self, column_names: impl IntoIterator<Item = impl ToString>) -> Self {
        self.column_names = Some(
            column_names
                .into_iter()
                .map(|item| item.to_string())
                .collect(),
        );
        self
    }

    pub fn snapshot_id(mut self, snapshot_id: i64) -> Self {
        self.snapshot_id = Some(snapshot_id);
        self
    }

    pub async fn build(self) -> Result<TableScan> {
        let snapshot = match self.snapshot_id {
            Some(snapshot_id) => self
                .table
                .snapshot_by_id(snapshot_id)
                .await?
                .ok_or_else(|| Error::DataInvalid {
                    message: format!("Snapshot with id {} not found", snapshot_id),
                    source: None,
                })?
                .clone(),
            None => self
                .table
                .current_snapshot()
                .await?
                .ok_or_else(|| Error::DataInvalid {
                    message: "Can't scan table without snapshots".to_string(),
                    source: None,
                })?
                .clone(),
        };

        let snapshot_ref = Arc::new(snapshot);

        let schema = self
            .table
            .schema_by_snapshot(snapshot_ref.clone())
            .await
            .unwrap();

        Ok(TableScan {
            snapshot: snapshot_ref.clone(),
            table_schema: Arc::new(schema),
            file_io: self.table.file_io().clone(),
            column_names: self.column_names.unwrap_or_default(),
            table_path: self.table.path().clone(),
            batch_size: None,
        })
    }
}

#[derive(Debug)]
pub struct TableScan {
    snapshot: SnapshotRef,
    table_schema: TableSchemaRef,
    file_io: FileIO,
    column_names: Vec<String>,
    table_path: String,
    batch_size: Option<usize>,
}

/// A stream of [`FileScanTask`].
pub type FileScanTaskStream = BoxStream<'static, crate::Result<FileScanTask>>;

impl TableScan {
    /// Plan files to scan based on the snapshot.
    ///
    /// This method reads manifest lists (base and delta) from the snapshot,
    /// then reads each manifest file to collect all data files that need to be scanned.
    /// Only files with FileKind::Add are included in the result.
    pub async fn plan_files(&self) -> crate::Result<FileScanTaskStream> {
        let table_path = PathBuf::from(&self.table_path);

        // Collect all manifest file paths from base and delta manifest lists
        let mut manifest_file_paths = Vec::new();

        // Read base manifest list
        let base_manifest_list_path = table_path.join("manifest").join(self.snapshot.base_manifest_list()).to_string_lossy().to_string();
        let base_manifest_list_content = self.read_file(base_manifest_list_path.as_str()).await?;
        let base_manifest_files: Vec<ManifestFileMeta> =
            from_avro_bytes(&base_manifest_list_content)?;
        for manifest_file_meta in base_manifest_files {
            manifest_file_paths.push(manifest_file_meta);
        }

        // Read delta manifest list
        // Manifest list files are stored directly in the table directory
        let delta_manifest_list_path = table_path.join("manifest").join(self.snapshot.delta_manifest_list()).to_string_lossy().to_string();
        let delta_manifest_list_content = self.read_file(&delta_manifest_list_path).await?;
        let delta_manifest_files: Vec<ManifestFileMeta> =
            from_avro_bytes(&delta_manifest_list_content)?;
        for manifest_file_meta in delta_manifest_files {
            manifest_file_paths.push(manifest_file_meta);
        }

        // Read all manifest files and collect data files
        let mut data_files = Vec::new();
        for manifest_meta in manifest_file_paths {
            let manifest_path = table_path.join("manifest").join(manifest_meta.file_name()).to_string_lossy().to_string();
            let entries = Manifest::read(&self.file_io, &manifest_path).await?;
            for entry in entries {
                // Only include files that are added (not deleted)
                if entry.kind() == &FileKind::Add {
                    let data_file_meta = entry.file();
                    // Data files are stored in bucket-X directories where X is the bucket number
                    let bucket_dir = format!("bucket-{}", entry.bucket());
                    // todo: consider path factory to resolve real path
                    let data_file_path = table_path
                        .join(&bucket_dir)
                        .join(&data_file_meta.file_name)
                        .to_string_lossy()
                        .to_string();
                    data_files.push(FileScanTask {
                        data_file_path,
                        start: 0, // Start from beginning of file
                        length: data_file_meta.file_size as u64, // Full file size
                    });
                }
            }
        }

        // Convert to stream
        Ok(Box::pin(stream::iter(data_files.into_iter().map(Ok))))
    }

    pub async fn to_arrow(&self) -> crate::Result<ArrowRecordBatchStream> {
        let mut arrow_reader_builder = ArrowReaderBuilder::new(self.file_io.clone());
        if let Some(batch_size) = self.batch_size {
            arrow_reader_builder = arrow_reader_builder.with_batch_size(batch_size);
        }

        arrow_reader_builder.build().read(self.plan_files().await?)
    }

    /// Helper method to read file content
    async fn read_file(&self, path: &str) -> crate::Result<Vec<u8>> {
        let input_file = self.file_io.new_input(path)?;
        if !input_file.exists().await? {
            return Ok(Vec::new());
        }
        let content = input_file.read().await?;
        Ok(content.to_vec())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileScanTask {
    data_file_path: String,
    start: u64,
    length: u64,
}

impl FileScanTask {
    /// Returns the data file path of this file scan task.
    pub fn data_file_path(&self) -> &str {
        &self.data_file_path
    }
}
