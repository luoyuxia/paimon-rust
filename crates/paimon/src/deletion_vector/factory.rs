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

use crate::deletion_vector::DeletionVector;
use crate::io::FileIO;
use crate::spec::{DataFileMeta, IndexManifestEntry};
use crate::Result;
use std::collections::HashMap;
use std::sync::Arc;

/// Factory for creating DeletionVector instances from files and metadata.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/deletionvectors/DeletionVector.java>
pub struct DeletionVectorFactory {
    file_io: FileIO,
    /// Map from data file name to its deletion vector
    deletion_vectors: HashMap<String, Arc<DeletionVector>>,
}

impl DeletionVectorFactory {
    /// Create a new DeletionVectorFactory
    pub fn new(file_io: FileIO) -> Self {
        Self {
            file_io,
            deletion_vectors: HashMap::new(),
        }
    }

    /// Create a DeletionVectorFactory from data files and deletion files.
    ///
    /// This method reads deletion vector information from index files and creates
    /// deletion vectors for each data file.
    pub async fn create(
        file_io: FileIO,
        data_files: &[DataFileMeta],
        deletion_files: Option<&[IndexManifestEntry]>,
        table_path: &str,
    ) -> Result<Self> {
        let mut factory = Self::new(file_io.clone());

        // If no deletion files, return empty factory
        let deletion_files = match deletion_files {
            Some(files) => files,
            None => return Ok(factory),
        };

        // Group deletion files by bucket and partition
        let mut deletion_files_by_bucket: HashMap<(Vec<u8>, i32), Vec<&IndexManifestEntry>> =
            HashMap::new();
        for entry in deletion_files {
            if entry.index_file.index_type == "DELETION_VECTORS" {
                let key = (entry.partition.clone(), entry.bucket);
                deletion_files_by_bucket
                    .entry(key)
                    .or_insert_with(Vec::new)
                    .push(entry);
            }
        }

        // Process each deletion file
        for (_, entries) in deletion_files_by_bucket {
            for entry in entries {
                if let Some(ranges) = &entry.index_file.deletion_vectors_ranges {
                    // Read the deletion vector index file
                    // Path: table_path/index/index_file_name
                    let index_file_path =
                        format!("{}/index/{}", table_path, entry.index_file.file_name);
                    let index_file = factory.file_io.new_input(&index_file_path)?;

                    if index_file.exists().await? {
                        // Read all deletion vectors from this index file
                        // Similar to DeletionVectorsIndexFile.readAllDeletionVectors
                        let deletion_vectors = factory
                            .read_all_deletion_vectors(&index_file, ranges)
                            .await?;

                        // Add to factory
                        for (data_file_name, deletion_vector) in deletion_vectors {
                            if data_files.iter().any(|f| f.file_name == data_file_name) {
                                factory
                                    .deletion_vectors
                                    .insert(data_file_name, Arc::new(deletion_vector));
                            }
                        }
                    }
                }
            }
        }

        Ok(factory)
    }

    /// Read all deletion vectors from an index file
    ///
    /// Similar to DeletionVectorsIndexFile.readAllDeletionVectors:
    /// 1. Check version (first byte should be VERSION_ID_V1 = 1)
    /// 2. For each range in deletion_vectors_ranges, read the deletion vector
    ///
    /// Format:
    /// - Version (1 byte): VERSION_ID_V1 = 1
    /// - For each deletion vector (in order of ranges):
    ///   - bitmapLength (4 bytes int): total size including magic
    ///   - magicNumber (4 bytes int): BitmapDeletionVector.MAGIC_NUMBER
    ///   - bitmap data (bitmapLength - 4 bytes)
    ///   - CRC (4 bytes)
    async fn read_all_deletion_vectors(
        &self,
        index_file: &crate::io::InputFile,
        ranges: &indexmap::IndexMap<String, (i32, i32)>,
    ) -> Result<std::collections::HashMap<String, DeletionVector>> {
        use crate::io::FileRead;
        use bytes::Buf;

        let reader = index_file.reader().await?;
        let metadata = index_file.metadata().await?;
        let mut all_data = reader.read(0..metadata.size).await?;

        // // Check version (first byte should be VERSION_ID_V1 = 1)
        // const VERSION_ID_V1: u8 = 1;
        // if all_data.len() < 1 {
        //     return Err(crate::Error::DataInvalid {
        //         message: "Deletion vector index file too short".to_string(),
        //         source: None,
        //     });
        // }
        //
        // // let version = all_data.get_u8();
        // // if version != VERSION_ID_V1 {
        // //     return Err(crate::Error::DataInvalid {
        // //         message: format!("Invalid version: expected {}, got {}", VERSION_ID_V1, version),
        // //         source: None,
        // //     });
        // // }

        let mut deletion_vectors = std::collections::HashMap::new();

        // Read deletion vectors in the order specified by ranges
        for (data_file_name, (offset, length)) in ranges {
            let start_pos = *offset as usize;

            // if all_data.len() < end_pos {
            //     return Err(crate::Error::DataInvalid {
            //         message: format!(
            //             "Deletion vector data incomplete for {}: need {} bytes, got {}",
            //             data_file_name, end_pos, all_data.len()
            //         ),
            //         source: None,
            //     });
            // }

            // Extract the deletion vector data
            let dv_data = &all_data[start_pos..];

            // Parse using DeletionVector.read format
            let deletion_vector = DeletionVector::read_from_bytes(dv_data, Some(*length as u64))?;

            deletion_vectors.insert(data_file_name.clone(), deletion_vector);
        }

        Ok(deletion_vectors)
    }

    /// Get the deletion vector for a specific data file
    pub fn get_deletion_vector(&self, data_file_name: &str) -> Option<Arc<DeletionVector>> {
        self.deletion_vectors.get(data_file_name).cloned()
    }

    /// Check if a row in a data file is deleted
    pub fn is_row_deleted(&self, data_file_name: &str, row_position: u64) -> bool {
        self.deletion_vectors
            .get(data_file_name)
            .map(|dv| dv.is_deleted(row_position))
            .unwrap_or(false)
    }
}
