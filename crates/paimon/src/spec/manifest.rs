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

use crate::error::{Error, Result};
use crate::io::FileIO;
use crate::spec::manifest_entry::ManifestEntry;
use crate::spec::manifest_file_meta::ManifestFileMeta;
use crate::spec::stats::BinaryTableStats;
use apache_avro::types::Value;
use apache_avro::{from_value, Reader};

/// Manifest file reader and writer.
///
/// A manifest file contains a list of ManifestEntry records in Avro format.
/// Each entry represents an addition or deletion of a data file.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/manifest/ManifestFile.java>
pub struct Manifest;

impl Manifest {
    /// Read manifest entries from a file.
    ///
    /// # Arguments
    /// * `file_io` - FileIO instance for reading files
    /// * `path` - Path to the manifest file
    ///
    /// # Returns
    /// A vector of ManifestEntry records
    pub async fn read(file_io: &FileIO, path: &str) -> Result<Vec<ManifestEntry>> {
        let input_file = file_io.new_input(path)?;

        if !input_file.exists().await? {
            return Ok(Vec::new());
        }

        let content = input_file.read().await?;
        Self::read_from_bytes(&content)
    }

    /// Read manifest entries from bytes.
    ///
    /// # Arguments
    /// * `bytes` - Avro-encoded manifest file content
    ///
    /// # Returns
    /// A vector of ManifestEntry records
    pub fn read_from_bytes(bytes: &[u8]) -> Result<Vec<ManifestEntry>> {
        let reader = Reader::new(bytes).map_err(Error::from)?;
        let records = reader
            .collect::<std::result::Result<Vec<Value>, _>>()
            .map_err(Error::from)?;
        let values = Value::Array(records);
        from_value::<Vec<ManifestEntry>>(&values).map_err(Error::from)
    }

    /// Write manifest entries to a file.
    ///
    /// # Arguments
    /// * `file_io` - FileIO instance for writing files
    /// * `path` - Path to write the manifest file
    /// * `entries` - Manifest entries to write
    ///
    /// # Returns
    /// The size of the written file in bytes
    pub async fn write(file_io: &FileIO, path: &str, entries: &[ManifestEntry]) -> Result<u64> {
        let bytes = Self::write_to_bytes(entries)?;
        let output_file = file_io.new_output(path)?;
        output_file.write(bytes.into()).await?;

        let input_file = output_file.to_input_file();
        let metadata = input_file.metadata().await?;
        Ok(metadata.size)
    }

    /// Write manifest entries to bytes.
    ///
    /// # Arguments
    /// * `entries` - Manifest entries to write
    ///
    /// # Returns
    /// Avro-encoded bytes
    ///
    /// # Note
    /// This is a placeholder implementation. Full Avro serialization requires
    /// proper schema generation and serialization logic.
    /// For now, this returns an error indicating that writing is not yet implemented.
    pub fn write_to_bytes(_entries: &[ManifestEntry]) -> Result<Vec<u8>> {
        // TODO: Implement proper Avro serialization
        // This requires:
        // 1. Proper Avro schema generation from ManifestEntry
        // 2. Correct serialization of all nested types (DataFileMeta, BinaryTableStats, etc.)
        // 3. Handling of nullable fields (delete_row_count, embedded_index)
        Err(Error::ConfigInvalid {
            message: "Manifest writing is not yet implemented. Use serde_avro_fast for full serialization support.".to_string(),
        })
    }

    /// Create a ManifestFileMeta from manifest entries.
    ///
    /// # Arguments
    /// * `file_name` - Name of the manifest file
    /// * `file_size` - Size of the manifest file in bytes
    /// * `entries` - Manifest entries to analyze
    /// * `schema_id` - Schema ID when writing this manifest file
    ///
    /// # Returns
    /// ManifestFileMeta with statistics
    pub fn create_manifest_file_meta(
        file_name: String,
        file_size: u64,
        entries: &[ManifestEntry],
        schema_id: i64,
    ) -> Result<ManifestFileMeta> {
        let mut num_added_files = 0i64;
        let mut num_deleted_files = 0i64;
        let partition_stats = BinaryTableStats::new(Vec::new(), Vec::new(), Vec::new());

        for entry in entries {
            match entry.kind() {
                crate::spec::manifest_common::FileKind::Add => {
                    num_added_files += 1;
                }
                crate::spec::manifest_common::FileKind::Delete => {
                    num_deleted_files += 1;
                }
            }
        }

        // TODO: Calculate partition stats from entries
        // For now, use empty stats

        Ok(ManifestFileMeta::new(
            file_name,
            file_size as i64,
            num_added_files,
            num_deleted_files,
            partition_stats,
            schema_id,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::FileIO;
    use crate::spec::manifest_common::FileKind;
    use crate::spec::DataFileMeta;
    use chrono::{DateTime, Utc};
    use std::env::current_dir;

    fn load_fixture(name: &str) -> Vec<u8> {
        let path = current_dir()
            .unwrap_or_else(|err| panic!("current_dir must exist: {err}"))
            .join(format!("tests/fixtures/manifest/{name}"));
        std::fs::read(&path).unwrap_or_else(|err| panic!("fixtures {path:?} load failed: {err}"))
    }

    #[test]
    fn test_read_manifest_from_bytes() {
        let bytes = load_fixture("manifest-8ded1f09-fcda-489e-9167-582ac0f9f846-0");
        let entries = Manifest::read_from_bytes(&bytes).unwrap();
        assert_eq!(entries.len(), 2);
    }

    #[tokio::test]
    async fn test_read_manifest_from_file() {
        let workdir = current_dir().unwrap();
        let path =
            workdir.join("tests/fixtures/manifest/manifest-8ded1f09-fcda-489e-9167-582ac0f9f846-0");

        let file_io = FileIO::from_url("file://").unwrap().build().unwrap();
        let entries = Manifest::read(&file_io, path.to_str().unwrap())
            .await
            .unwrap();
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn test_create_manifest_file_meta() {
        let entries = vec![ManifestEntry::new(
            FileKind::Add,
            vec![0, 0, 0, 1],
            0,
            10,
            DataFileMeta {
                file_name: "test.parquet".to_string(),
                file_size: 100,
                row_count: 10,
                min_key: vec![0],
                max_key: vec![100],
                key_stats: BinaryTableStats::new(vec![], vec![], vec![]),
                value_stats: BinaryTableStats::new(vec![], vec![], vec![]),
                min_sequence_number: 1,
                max_sequence_number: 10,
                schema_id: 0,
                level: 0,
                extra_files: vec![],
                creation_time: DateTime::from_timestamp(0, 0).unwrap().with_timezone(&Utc),
                delete_row_count: None,
                embedded_index: None,
            },
            2,
        )];

        let meta =
            Manifest::create_manifest_file_meta("manifest-test".to_string(), 1000, &entries, 0)
                .unwrap();

        assert_eq!(meta.file_name(), "manifest-test");
        assert_eq!(meta.file_size(), 1000);
        assert_eq!(meta.num_added_files(), 1);
        assert_eq!(meta.num_deleted_files(), 0);
    }
}
