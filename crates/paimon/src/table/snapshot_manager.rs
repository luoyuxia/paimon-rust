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

//! Snapshot manager for reading snapshot metadata using FileIO.
//!
//! Reference: [pypaimon.snapshot.snapshot_manager.SnapshotManager](https://github.com/apache/paimon/blob/master/paimon-python/pypaimon/snapshot/snapshot_manager.py).

use crate::io::FileIO;
use crate::spec::Snapshot;
use std::str;

const SNAPSHOT_DIR: &str = "snapshot";
const LATEST_SNAPSHOT_FILE: &str = "LATEST";

/// Manager for snapshot files using unified FileIO.
///
/// Reference: [pypaimon.snapshot.snapshot_manager.SnapshotManager](https://github.com/apache/paimon/blob/master/paimon-python/pypaimon/snapshot/snapshot_manager.py).
#[derive(Debug, Clone)]
pub struct SnapshotManager {
    file_io: FileIO,
    table_path: String,
}

impl SnapshotManager {
    /// Create a snapshot manager for the given table path and FileIO.
    pub fn new(file_io: FileIO, table_path: String) -> Self {
        Self {
            file_io,
            table_path,
        }
    }

    /// Path to the snapshot directory (e.g. `table_path/snapshot`).
    pub fn snapshot_dir(&self) -> String {
        format!("{}/{}", self.table_path, SNAPSHOT_DIR)
    }

    /// Path to the LATEST file that stores the latest snapshot id.
    pub fn latest_file_path(&self) -> String {
        format!("{}/{}", self.snapshot_dir(), LATEST_SNAPSHOT_FILE)
    }

    /// Path to the snapshot file for the given id (e.g. `snapshot/snapshot-1`).
    pub fn snapshot_path(&self, snapshot_id: i64) -> String {
        format!("{}/snapshot-{}", self.snapshot_dir(), snapshot_id)
    }

    /// Get the latest snapshot, or None if LATEST does not exist.
    /// Returns an error if LATEST exists but the snapshot file (snapshot-{id}) does not exist.
    pub async fn get_latest_snapshot(&self) -> crate::Result<Option<Snapshot>> {
        // todo: consider snapshot loader to load snapshot from catalog
        let latest_path = self.latest_file_path();
        let input = self.file_io.new_input(&latest_path)?;
        if !input.exists().await? {
            // todo: may need to list directory and find the latest snapshot
            return Ok(None);
        }
        let content = input.read().await?;
        let id_str = str::from_utf8(&content).map_err(|e| crate::Error::ConfigInvalid {
            message: format!("LATEST snapshot file invalid utf8: {e}"),
        })?;
        let snapshot_id: i64 = id_str
            .trim()
            .parse()
            .map_err(|_| crate::Error::ConfigInvalid {
                message: format!("LATEST snapshot id not a number: {id_str:?}"),
            })?;
        let snapshot_path = self.snapshot_path(snapshot_id);
        let snap_input = self.file_io.new_input(&snapshot_path)?;
        if !snap_input.exists().await? {
            return Err(crate::Error::ConfigInvalid {
                message: format!(
                    "snapshot file does not exist: {snapshot_path} (LATEST points to snapshot id {snapshot_id})"
                ),
            });
        }
        let snap_bytes = snap_input.read().await?;
        let snapshot: Snapshot =
            serde_json::from_slice(&snap_bytes).map_err(|e| crate::Error::ConfigInvalid {
                message: format!("snapshot JSON invalid: {e}"),
            })?;
        Ok(Some(snapshot))
    }
}
