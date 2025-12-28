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

//! FileSystem-based catalog implementation.
//!
//! This catalog stores table metadata in the file system.
//! Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/catalog/FileSystemCatalog.java>

use std::fmt::format;
use async_trait::async_trait;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::catalog::{Catalog, Identifier, Schema, Table};
use crate::error::{Error, Result};
use crate::io::FileIO;
use crate::spec::{SchemaChange, Snapshot, TableSchema};

/// FileSystem-based catalog implementation.
///
/// Tables are stored in the file system with the following structure:
/// ```
/// <warehouse_path>/
///   <database>/
///     <table>/
///       schema/
///       snapshot/
///       manifest/
///       ...
/// ```
#[derive(Debug)]
pub struct FileSystemCatalog {
    warehouse_path: PathBuf,
    file_io: Arc<FileIO>,
}

impl FileSystemCatalog {
    
    const DB_SUFFIX: &'static str = ".db";
    const DEFAULT_DATABASE: &'static str = "default";
    
    /// Create a new FileSystemCatalog.
    ///
    /// # Arguments
    /// * `name` - Name of the catalog
    /// * `warehouse_path` - Root path where tables are stored
    /// * `file_io` - FileIO instance for file operations
    pub fn new(warehouse_path: impl Into<PathBuf>, file_io: FileIO) -> Self {
        Self {
            warehouse_path: warehouse_path.into(),
            file_io: Arc::new(file_io),
        }
    }

    /// Get the warehouse path.
    pub fn warehouse_path(&self) -> &Path {
        &self.warehouse_path
    }

    fn database_path(&self, database: &str) -> PathBuf {
        self.warehouse_path.join(format!("{}{}", database, Self::DB_SUFFIX))
    }

    fn table_path(&self, identifier: &Identifier) -> PathBuf {
        self.database_path(&identifier.database)
            .join(&identifier.table)
    }

    fn schema_path(&self, identifier: &Identifier) -> PathBuf {
        self.table_path(identifier).join("schema")
    }

    fn snapshot_path(&self, identifier: &Identifier) -> PathBuf {
        self.table_path(identifier).join("snapshot")
    }

    fn current_snapshot_path(&self, identifier: &Identifier) -> PathBuf {
        self.snapshot_path(identifier).join("LATEST")
    }

    /// Get table location path for an identifier.
    /// Equivalent to `getTableLocation(identifier)` in Java.
    fn get_table_location(&self, identifier: &Identifier) -> PathBuf {
        self.table_path(identifier)
    }

    /// Get schema directory path.
    fn schema_directory(&self, table_location: &Path) -> PathBuf {
        table_location.join("schema")
    }

    /// List versioned schema files and find the latest version.
    /// Equivalent to `listVersionedFiles(fileIO, schemaDirectory(), SCHEMA_PREFIX).reduce(Math::max)` in Java.
    async fn latest_schema_version(&self, schema_dir: &Path) -> Option<i64> {
        const SCHEMA_PREFIX: &str = "schema-";
        
        let schema_dir = format!("{}/", schema_dir.to_string_lossy());
        
        let statuses = match self.file_io.list_status(&schema_dir).await {
            Ok(statuses) => statuses,
            Err(_) => return None,
        };

        let mut max_version: Option<i64> = None;

        for status in statuses {
            if status.is_dir {
                continue;
            }

            // Extract filename from path
            let file_name = Path::new(&status.path)
                .file_name()
                .and_then(|n| n.to_str())?;

            // Check if file matches schema-{version} pattern
            if let Some(version_str) = file_name.strip_prefix(SCHEMA_PREFIX) {
                if let Ok(version) = version_str.parse::<i64>() {
                    max_version = Some(max_version.map_or(version, |v| v.max(version)));
                }
            }
        }

        max_version
    }

    /// Load schema by version.
    /// Equivalent to `schema(version)` in Java SchemaManager.
    async fn load_schema_by_version(&self, schema_dir: &Path, version: i64) -> Result<TableSchema> {
        const SCHEMA_PREFIX: &str = "schema-";
        let schema_file_path = schema_dir.join(format!("{}{}", SCHEMA_PREFIX, version));
        
        let schema_file =  self.file_io.new_input(&schema_file_path.to_string_lossy())?;

        let content = schema_file.read().await?;

        serde_json::from_slice(&content).map_err(|e|  {
            Error::DataInvalid {
                message: "Fail to parse table schema from schema file".to_string(),
                source: Some(Box::new(e)),
            }
        })
    }

    /// Load schema from default schema file (non-versioned).
    /// Fallback when no versioned schema files are found.
    async fn load_default_schema(&self, schema_dir: &Path) -> Option<TableSchema> {
        let schema_file_path = schema_dir;
        
        let schema_file = match self.file_io.new_input(&schema_file_path.to_string_lossy()) {
            Ok(file) => file,
            Err(_) => return None,
        };

        if !schema_file.exists().await.ok()? {
            return None;
        }

        let content = match schema_file.read().await {
            Ok(bytes) => bytes,
            Err(_) => return None,
        };

        serde_json::from_slice(&content).ok()
    }

    /// Load table schema from file system.
    /// Equivalent to `tableSchemaInFileSystem(tableLocation, branchName)` in Java.
    /// Returns `None` if the schema file doesn't exist or can't be read.
    async fn table_schema_in_file_system(
        &self,
        table_location: &Path,
        branch_name: &str,
    ) -> Result<Option<TableSchema>> {
        const DEFAULT_MAIN_BRANCH: &str = "";
        const PATH_KEY: &str = "path";
        const BRANCH_KEY: &str = "branch";

        let schema_dir = self.schema_directory(table_location);
        
        let latest_version = self.latest_schema_version(&schema_dir).await;
        if latest_version.is_none() {
           return Ok(None)
        }
        let mut schema = self.load_schema_by_version(&schema_dir,
                                                 latest_version.unwrap()).await?;

        // If not default branch, add BRANCH option
        if branch_name != DEFAULT_MAIN_BRANCH {
            let mut new_options = schema.options().clone();
            new_options.insert(BRANCH_KEY.to_string(), branch_name.to_string());
            schema = schema.with_options(new_options);
        }

        // Add PATH option
        let mut final_options = schema.options().clone();
        final_options.insert(PATH_KEY.to_string(), table_location.to_string_lossy().to_string());
        schema = schema.with_options(final_options);

        Ok(Some(schema))
    }

    async fn read_schema(&self, identifier: &Identifier) -> Result<TableSchema> {
        let table_location = self.get_table_location(identifier);
        // For now, use empty string as default branch name since branch support is not yet implemented
        let branch_name = ""; // identifier.getBranchNameOrDefault() equivalent

        self.table_schema_in_file_system(&table_location, branch_name).await?
            .ok_or(Error::DataInvalid {
                message: "table don't exist".to_string(),
                source: None,
            })
    }

    async fn write_schema(&self, identifier: &Identifier, schema: &TableSchema) -> Result<()> {
        let schema_path = self.schema_path(identifier);
        let schema_file = self.file_io.new_output(&schema_path.to_string_lossy())?;

        let content = serde_json::to_string_pretty(schema).map_err(|e| Error::DataInvalid {
            message: format!("Failed to serialize schema for table {:?}", identifier),
            source: Some(Box::new(e)),
        })?;

        schema_file.write(content.into()).await?;
        Ok(())
    }

    async fn read_current_snapshot(&self, identifier: &Identifier) -> Result<Option<Snapshot>> {
        let snapshot_path = self.current_snapshot_path(identifier);
        let snapshot_file = self.file_io.new_input(&snapshot_path.to_string_lossy());

        match snapshot_file {
            Ok(file) => {
                if !file.exists().await? {
                    return Ok(None);
                }
                let content = file.read().await?;
                
                // LATEST file may contain either:
                // 1. A snapshot ID (integer) - need to read the snapshot file
                // 2. A full snapshot JSON object
                // Try to parse as integer first
                if let Ok(snapshot_id_str) = String::from_utf8(content.clone().to_vec()) {
                    if let Ok(snapshot_id) = snapshot_id_str.trim().parse::<i64>() {
                        // LATEST contains snapshot ID, read the actual snapshot file
                        let actual_snapshot_path = self.snapshot_path(identifier).join(snapshot_id.to_string());
                        let actual_snapshot_file = self.file_io.new_input(&actual_snapshot_path.to_string_lossy())?;
                        if !actual_snapshot_file.exists().await? {
                            return Ok(None);
                        }
                        let snapshot_content = actual_snapshot_file.read().await?;
                        let snapshot: Snapshot =
                            serde_json::from_slice(&snapshot_content).map_err(|e| Error::DataInvalid {
                                message: format!("Failed to parse snapshot for table {:?}", identifier),
                                source: Some(Box::new(e)),
                            })?;
                        return Ok(Some(snapshot));
                    }
                }
                
                // Try to parse as full snapshot JSON
                let snapshot: Snapshot =
                    serde_json::from_slice(&content).map_err(|e| Error::DataInvalid {
                        message: format!("Failed to parse snapshot for table {:?}", identifier),
                        source: Some(Box::new(e)),
                    })?;
                Ok(Some(snapshot))
            }
            Err(_) => Ok(None),
        }
    }
}

#[async_trait]
impl Catalog for FileSystemCatalog {
    async fn database_exists(&self, database: &str) -> Result<bool> {
        let db_path = self.database_path(database);
        self.file_io.exists(&db_path.to_string_lossy()).await
    }

    async fn list_databases(&self) -> Result<Vec<String>> {
        let statuses = self
            .file_io
            .list_status(&self.warehouse_path.to_string_lossy())
            .await?;

        let mut databases = Vec::new();
        for status in statuses {
            if status.is_dir {
                if let Some(name) = Path::new(&status.path).file_name() {
                    databases.push(name.to_string_lossy().to_string());
                }
            }
        }

        Ok(databases)
    }

    async fn create_database(&self, database: &str, ignore_if_exists: bool) -> Result<()> {
        let db_path = self.database_path(database);
        let exists = self.database_exists(database).await?;

        if exists {
            if ignore_if_exists {
                return Ok(());
            }
            return Err(Error::ConfigInvalid {
                message: format!("Database '{}' already exists", database),
            });
        }

        self.file_io.mkdirs(&db_path.to_string_lossy()).await?;
        Ok(())
    }

    async fn drop_database(
        &self,
        database: &str,
        ignore_if_not_exists: bool,
        cascade: bool,
    ) -> Result<()> {
        let db_path = self.database_path(database);
        let exists = self.database_exists(database).await?;

        if !exists {
            if ignore_if_not_exists {
                return Ok(());
            }
            return Err(Error::ConfigInvalid {
                message: format!("Database '{}' does not exist", database),
            });
        }

        if cascade {
            // Check if database has tables
            let tables = self.list_tables(database).await?;
            if !tables.is_empty() {
                return Err(Error::ConfigInvalid {
                    message: format!(
                        "Cannot drop database '{}' with {} tables. Use CASCADE to force drop.",
                        database,
                        tables.len()
                    ),
                });
            }
        }

        self.file_io.delete_dir(&db_path.to_string_lossy()).await?;
        Ok(())
    }

    async fn table_exists(&self, identifier: &Identifier) -> Result<bool> {
        let table_path = self.table_path(identifier);
        self.file_io.exists(&table_path.to_string_lossy()).await
    }

    async fn list_tables(&self, database: &str) -> Result<Vec<String>> {
        if !self.database_exists(database).await? {
            return Err(Error::ConfigInvalid {
                message: format!("Database '{}' does not exist", database),
            });
        }

        let db_path = self.database_path(database).join("/");
        let statuses = self.file_io.list_status(&db_path.to_string_lossy()).await?;

        let mut tables = Vec::new();
        for status in statuses {
            if status.is_dir {
                if let Some(name) = Path::new(&status.path).file_name() {
                    tables.push(name.to_string_lossy().to_string());
                }
            }
        }

        Ok(tables)
    }

    async fn get_table(&self, identifier: &Identifier) -> Result<Table> {
        if !self.table_exists(identifier).await? {
            return Err(Error::ConfigInvalid {
                message: format!(
                    "Table '{}.{}' does not exist",
                    identifier.database, identifier.table
                ),
            });
        }

        let schema = self.read_schema(identifier).await?;
        let table_path = self.table_path(identifier);

        Ok(Table::builder()
            .path(table_path.to_string_lossy().to_string())
            .table_schema(schema)
            .file_io((*self.file_io).clone())
            .identifier(identifier.clone())
            .build())
    }

    async fn create_table(
        &self,
        identifier: &Identifier,
        schema: Schema,
        ignore_if_exists: bool,
    ) -> Result<Table> {
        let exists = self.table_exists(identifier).await?;

        if exists {
            if ignore_if_exists {
                return self.get_table(identifier).await;
            }
            return Err(Error::ConfigInvalid {
                message: format!(
                    "Table '{}.{}' already exists",
                    identifier.database, identifier.table
                ),
            });
        }

        // Ensure database exists
        if !self.database_exists(&identifier.database).await? {
            self.create_database(&identifier.database, false).await?;
        }

        // Convert Schema to TableSchema
        let table_schema = schema.to_table_schema()?;

        // Create table directory
        let table_path = self.table_path(identifier);
        self.file_io.mkdirs(&table_path.to_string_lossy()).await?;

        // Write schema file
        self.write_schema(identifier, &table_schema).await?;

        // Return Table instance
        Ok(Table::builder()
            .path(table_path.to_string_lossy().to_string())
            .table_schema(table_schema)
            .file_io((*self.file_io).clone())
            .identifier(identifier.clone())
            .build())
    }

    async fn drop_table(&self, identifier: &Identifier, ignore_if_not_exists: bool) -> Result<()> {
        let exists = self.table_exists(identifier).await?;

        if !exists {
            if ignore_if_not_exists {
                return Ok(());
            }
            return Err(Error::ConfigInvalid {
                message: format!(
                    "Table '{}.{}' does not exist",
                    identifier.database, identifier.table
                ),
            });
        }

        let table_path = self.table_path(identifier);
        self.file_io
            .delete_dir(&table_path.to_string_lossy())
            .await?;
        Ok(())
    }

    async fn alter_table(
        &self,
        _identifier: &Identifier,
        _changes: Vec<SchemaChange>,
    ) -> Result<()> {
        // TODO: Implement schema alteration logic
        // This would involve:
        // 1. Reading current schema
        // 2. Applying changes
        // 3. Writing new schema
        Err(Error::ConfigInvalid {
            message: "Schema alteration not yet implemented".to_string(),
        })
    }

    async fn rename_table(
        &self,
        from: &Identifier,
        to: &Identifier,
        ignore_if_not_exists: bool,
    ) -> Result<()> {
        let exists = self.table_exists(from).await?;

        if !exists {
            if ignore_if_not_exists {
                return Ok(());
            }
            return Err(Error::ConfigInvalid {
                message: format!("Table '{}.{}' does not exist", from.database, from.table),
            });
        }

        if self.table_exists(to).await? {
            return Err(Error::ConfigInvalid {
                message: format!("Table '{}.{}' already exists", to.database, to.table),
            });
        }

        // Ensure target database exists
        if !self.database_exists(&to.database).await? {
            self.create_database(&to.database, false).await?;
        }

        let from_path = self.table_path(from);
        let to_path = self.table_path(to);

        self.file_io
            .rename(&from_path.to_string_lossy(), &to_path.to_string_lossy())
            .await?;
        Ok(())
    }
}
