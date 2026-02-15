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

//! TableScan for full table scan.
//!
//! Reference: [pypaimon.read.table_scan.TableScan](https://github.com/apache/paimon/blob/master/paimon-python/pypaimon/read/table_scan.py)
//! and [FullStartingScanner](https://github.com/apache/paimon/blob/master/paimon-python/pypaimon/read/scanner/full_starting_scanner.py).

use crate::io::FileIO;
use crate::spec::{FileKind, ManifestEntry, Snapshot};
use crate::table::source::{DataSplitBuilder, Plan};
use crate::table::SnapshotManager;
use std::collections::{HashMap, HashSet};

use super::Table;

/// Path segment for manifest directory under table.
const MANIFEST_DIR: &str = "manifest";

/// Reads a manifest list file (Avro) and returns manifest file metas.
async fn read_manifest_list(
    file_io: &FileIO,
    table_path: &str,
    list_name: &str,
) -> crate::Result<Vec<crate::spec::ManifestFileMeta>> {
    let path = format!(
        "{}/{}/{}",
        table_path.trim_end_matches('/'),
        MANIFEST_DIR,
        list_name
    );
    let input = file_io.new_input(&path)?;
    if !input.exists().await? {
        return Ok(Vec::new());
    }
    let bytes = input.read().await?;
    crate::spec::from_avro_bytes::<crate::spec::ManifestFileMeta>(&bytes)
}

/// Reads all manifest entries for a snapshot (base + delta manifest lists, then each manifest file).
async fn read_all_manifest_entries(
    file_io: &FileIO,
    table_path: &str,
    snapshot: &Snapshot,
) -> crate::Result<Vec<ManifestEntry>> {
    let mut manifest_files =
        read_manifest_list(file_io, table_path, snapshot.base_manifest_list()).await?;
    let delta = read_manifest_list(file_io, table_path, snapshot.delta_manifest_list()).await?;
    manifest_files.extend(delta);

    let manifest_path_prefix = format!("{}/{}", table_path.trim_end_matches('/'), MANIFEST_DIR);
    let mut all_entries = Vec::new();
    // todo: consider use multiple-threads read manifest
    for meta in manifest_files {
        let path = format!("{}/{}", manifest_path_prefix, meta.file_name());
        let entries = crate::spec::Manifest::read(file_io, &path).await?;
        all_entries.extend(entries);
    }
    Ok(all_entries)
}

/// Merges add/delete manifest entries: keeps only ADD entries whose (partition, bucket, file_name) is not in DELETE set.
fn merge_manifest_entries(entries: Vec<ManifestEntry>) -> Vec<ManifestEntry> {
    let mut deleted = HashSet::new();
    let mut added = Vec::new();
    for e in entries {
        // follow python code to use partition, bucket, filename as duplicator
        let key = (
            e.partition().to_vec(),
            e.bucket(),
            e.file().file_name.clone(),
        );
        match e.kind() {
            FileKind::Add => added.push(e),
            FileKind::Delete => {
                deleted.insert(key);
            }
        }
    }
    added
        .into_iter()
        .filter(|e| {
            !deleted.contains(&(
                e.partition().to_vec(),
                e.bucket(),
                e.file().file_name.clone(),
            ))
        })
        .collect()
}

/// TableScan for full table scan (no incremental, no predicate).
///
/// Reference: [pypaimon.read.table_scan.TableScan](https://github.com/apache/paimon/blob/master/paimon-python/pypaimon/read/table_scan.py)
#[derive(Debug, Clone)]
pub struct TableScan {
    table: Table,
}

impl TableScan {
    pub fn new(table: Table) -> Self {
        Self { table }
    }

    /// Plan the full scan: read latest snapshot, manifest list, manifest entries, then build one DataSplit per (partition, bucket).
    pub async fn plan(&self) -> crate::Result<Plan> {
        let file_io = self.table.file_io();
        let table_path = self.table.location();
        let snapshot_manager = SnapshotManager::new(file_io.clone(), table_path.to_string());

        let snapshot = match snapshot_manager.get_latest_snapshot().await? {
            Some(s) => s,
            None => return Ok(Plan::new(Vec::new())),
        };
        Self::plan_snapshot(snapshot, file_io, table_path).await
    }

    pub async fn plan_snapshot(
        snapshot: Snapshot,
        file_io: &FileIO,
        table_path: &str,
    ) -> crate::Result<Plan> {
        let entries = read_all_manifest_entries(file_io, table_path, &snapshot).await?;
        let entries = merge_manifest_entries(entries);
        if entries.is_empty() {
            return Ok(Plan::new(Vec::new()));
        }

        // Group by (partition, bucket). Key = (partition_bytes, bucket).
        let mut groups: HashMap<(Vec<u8>, i32), Vec<ManifestEntry>> = HashMap::new();
        for e in entries {
            let key = (e.partition().to_vec(), e.bucket());
            groups.entry(key).or_default().push(e);
        }

        let snapshot_id = snapshot.id();
        let base_path = table_path;
        let mut splits = Vec::new();

        for ((_partition, bucket), group_entries) in groups {
            let total_buckets = group_entries
                .first()
                .map(|e| e.total_buckets())
                .unwrap_or(-1);
            let mut data_files = Vec::new();

            // todo: consider use binpack to generate split
            for manifest_entry in group_entries {
                let ManifestEntry { file, .. } = manifest_entry;
                data_files.push(file);
            }
            let bucket_path = format!("{base_path}/bucket-{bucket}");
            let partition = crate::spec::EMPTY_BINARY_ROW.clone();

            let split = DataSplitBuilder::new()
                .with_snapshot(snapshot_id)
                .with_partition(partition)
                .with_bucket(bucket)
                .with_bucket_path(bucket_path)
                .with_total_buckets(total_buckets)
                .with_data_files(data_files)
                .build()?;
            splits.push(split);
        }
        Ok(Plan::new(splits))
    }
}
