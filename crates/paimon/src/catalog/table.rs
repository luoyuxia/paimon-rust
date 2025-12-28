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
// software distributed under this License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Table trait and implementation for Paimon tables.

use crate::catalog::Identifier;
use crate::io::FileIO;
use crate::scan::TableScanBuilder;
use crate::spec::{Snapshot, SnapshotRef, TableSchema};
use std::path::PathBuf;
use futures::TryFutureExt;
use typed_builder::TypedBuilder;
use crate::Error;
use crate::Result;

/// Table represents a table in the catalog.
#[derive(TypedBuilder, Debug, Clone)]
pub struct Table {
    file_io: FileIO,
    path: String,
    table_schema: TableSchema,
    identifier: Identifier,
}

impl Table {
    pub fn identifier(&self) -> &Identifier {
        &self.identifier
    }

    pub fn file_io(&self) -> &FileIO {
        &self.file_io
    }

    pub fn path(&self) -> &String {
        &self.path
    }
    
    pub fn table_schema(&self) -> &TableSchema {
        &self.table_schema
    }

    /// Get a snapshot by its ID.
    ///
    /// # Arguments
    /// * `snapshot_id` - The ID of the snapshot to retrieve
    ///
    /// # Returns
    /// The Snapshot with the given ID, or None if not found
    pub async fn snapshot_by_id(&self, snapshot_id: i64) -> Result<Option<Snapshot>> {
        let snapshot_path = PathBuf::from(&self.path)
            .join("snapshot")
            .join(format!("snapshot-{}", snapshot_id.to_string()));

        let snapshot_file = self.file_io.new_input(&snapshot_path.to_string_lossy())?;

        if !snapshot_file.exists().await? {
            return Ok(None);
        }

        let content = snapshot_file.read().await?;

        serde_json::from_slice(&content).map_err(|e| Error::DataInvalid {
            message: "Fail to parse snapshot ".to_string(),
            source: Some(Box::new(e)),
        })
    }

    /// Get the current snapshot.
    ///
    /// # Returns
    /// The current Snapshot, or None if not found
    pub async fn current_snapshot(&self) -> Result<Option<Snapshot>> {
        // todo: real latest should consider if LATEST hint doesn't exist
        let snapshot_latest_hint_path = PathBuf::from(&self.path).join("snapshot").join("LATEST");
        let snapshot_latest_hint_file = self.file_io.new_input(&snapshot_latest_hint_path.to_string_lossy())?;
        if !snapshot_latest_hint_file.exists().await? {
            return Err(Error::DataInvalid {
                message: "latest snapsot doesn't exist".to_string(),
                source: None,
            })
        }

        let content = snapshot_latest_hint_file.read().await?.to_vec();
        
        if let Ok(snapshot_id_str) = String::from_utf8(content.clone()) {
            if let Ok(snapshot_id) = snapshot_id_str.trim().parse::<i64>() {
                // LATEST contains snapshot ID, read the actual snapshot file
                return if let Some(snapshot) = self.snapshot_by_id(snapshot_id).await? {
                    Ok(Some(snapshot))
                } else {
                    Err(Error::DataInvalid {
                        message: format!("Failed to read snapshot with ID {}", snapshot_id),
                        source: None,
                    })
                }
            }
        }
        
        // Try to parse as full snapshot JSON
        let snapshot: Snapshot = serde_json::from_slice(&content).map_err(|e| {
            Error::DataInvalid {
                message: "Fail to parse snapshot".to_string(),
                source: Some(Box::new(e)),
            }
        })?;
        Ok(Some(snapshot))
    }

    /// Get the schema for a given snapshot.
    ///
    /// # Arguments
    /// * `snapshot` - The snapshot reference containing the schema_id
    ///
    /// # Returns
    /// The TableSchema for the snapshot, or None if not found
    pub async fn schema_by_snapshot(&self, snapshot: SnapshotRef) -> Option<TableSchema> {
        let schema_id = snapshot.schema_id();

        // Try to read schema file with schema_id first
        let schema_path_with_id = PathBuf::from(&self.path)
            .join("schema")
            .join(format!("schema-{}", schema_id));

        if let Ok(schema_file) = self
            .file_io
            .new_input(&schema_path_with_id.to_string_lossy())
        {
            if schema_file.exists().await.ok()? {
                if let Ok(content) = schema_file.read().await {
                    if let Ok(schema) = serde_json::from_slice::<TableSchema>(&content.to_vec()) {
                        if schema.id() == schema_id {
                            return Some(schema);
                        }
                    }
                }
            }
        }

        // Fallback to reading the default schema file
        let schema_path = PathBuf::from(&self.path).join("schema");
        let schema_file = match self.file_io.new_input(&schema_path.to_string_lossy()) {
            Ok(file) => file,
            Err(_) => return None,
        };

        if !schema_file.exists().await.ok()? {
            return None;
        }

        let content = match schema_file.read().await {
            Ok(bytes) => bytes.to_vec(),
            Err(_) => return None,
        };

        let schema: TableSchema = serde_json::from_slice(&content).ok()?;

        // Verify schema_id matches if available
        if schema.id() == schema_id {
            Some(schema)
        } else {
            // Return the schema anyway if IDs don't match (for backward compatibility)
            Some(schema)
        }
    }

    pub fn scan(&self) -> TableScanBuilder<'_> {
        TableScanBuilder::new(self)
    }
}
