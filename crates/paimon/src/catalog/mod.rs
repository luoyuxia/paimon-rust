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

//! Catalog API for managing databases and tables in Paimon.
//!
//! The Catalog API provides a unified interface for:
//! - Database/Namespace management (create, list, drop databases)
//! - Table management (create, list, get, drop tables)
//!
//! Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/catalog/Catalog.java>

mod table;
pub use table::*;

mod filesystem;
pub use filesystem::*;

use crate::error::Result;
use crate::spec::DataType;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Display;

/// Identifier for a table in the catalog.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct Identifier {
    /// Database name
    pub database: String,
    /// Table name
    pub table: String,
}

impl Identifier {
    /// Create a new table identifier.
    pub fn new(database: impl Into<String>, table: impl Into<String>) -> Self {
        Self {
            database: database.into(),
            table: table.into(),
        }
    }
}

impl Display for Identifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.database, self.table)
    }
}

/// Catalog trait for managing databases and tables.
///
/// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/catalog/Catalog.java>
#[async_trait]
pub trait Catalog: Send + Sync {
    /// Check if a database exists.
    async fn database_exists(&self, database: &str) -> Result<bool>;

    /// List all databases.
    async fn list_databases(&self) -> Result<Vec<String>>;

    /// Create a database.
    async fn create_database(&self, database: &str, ignore_if_exists: bool) -> Result<()>;

    /// Drop a database.
    async fn drop_database(
        &self,
        database: &str,
        ignore_if_not_exists: bool,
        cascade: bool,
    ) -> Result<()>;

    /// Check if a table exists.
    async fn table_exists(&self, identifier: &Identifier) -> Result<bool>;

    /// List all tables in a database.
    async fn list_tables(&self, database: &str) -> Result<Vec<String>>;

    /// Get a table by identifier.
    async fn get_table(&self, identifier: &Identifier) -> Result<Table>;

    /// Create a table.
    async fn create_table(
        &self,
        identifier: &Identifier,
        schema: Schema,
        ignore_if_exists: bool,
    ) -> Result<Table>;

    /// Drop a table.
    async fn drop_table(&self, identifier: &Identifier, ignore_if_not_exists: bool) -> Result<()>;

    /// Alter a table.
    async fn alter_table(
        &self,
        identifier: &Identifier,
        changes: Vec<crate::spec::SchemaChange>,
    ) -> Result<()>;

    /// Rename a table.
    async fn rename_table(
        &self,
        from: &Identifier,
        to: &Identifier,
        ignore_if_not_exists: bool,
    ) -> Result<()>;
}

/// Schema definition for creating a table.
///
/// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/schema/Schema.java>
#[derive(Debug, Clone)]
pub struct Schema {
    /// Column definitions: (name, data_type, description)
    pub fields: Vec<(String, DataType, Option<String>)>,
    /// Primary key column names
    pub primary_keys: Vec<String>,
    /// Partition key column names
    pub partition_keys: Vec<String>,
    /// Table options
    pub options: HashMap<String, String>,
    /// Table comment
    pub comment: Option<String>,
}

impl Schema {
    /// Create a new Schema builder.
    pub fn builder() -> SchemaBuilder {
        SchemaBuilder::new()
    }

    /// Convert Schema to TableSchema.
    ///
    /// This assigns field IDs starting from 0 and generates a schema ID.
    pub fn to_table_schema(&self) -> crate::Result<crate::spec::TableSchema> {
        use crate::error::Error;
        use crate::spec::DataField;
        use std::time::{SystemTime, UNIX_EPOCH};

        // Validate that all primary keys and partition keys exist in fields
        let field_names: std::collections::HashSet<String> = self
            .fields
            .iter()
            .map(|(name, _, _)| name.clone())
            .collect();

        for pk in &self.primary_keys {
            if !field_names.contains(pk) {
                return Err(Error::ConfigInvalid {
                    message: format!("Primary key '{}' does not exist in schema fields", pk),
                });
            }
        }

        for pk in &self.partition_keys {
            if !field_names.contains(pk) {
                return Err(Error::ConfigInvalid {
                    message: format!("Partition key '{}' does not exist in schema fields", pk),
                });
            }
        }

        // Assign field IDs starting from 0
        let fields: Vec<DataField> = self
            .fields
            .iter()
            .enumerate()
            .map(|(idx, (name, data_type, description))| {
                DataField::new(idx as i32, name.clone(), data_type.clone())
                    .with_description(description.clone())
            })
            .collect();

        // Calculate highest field ID
        let highest_field_id = if fields.is_empty() {
            0
        } else {
            fields.len() as i32 - 1
        };

        // Generate schema ID (use current timestamp in milliseconds)
        let schema_id = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        // Get current time in milliseconds
        let time_millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        // Create TableSchema using serde_json to deserialize from a JSON representation
        // Since TableSchema fields are private, we need to construct it via serialization
        // Serialize fields to JSON first to ensure proper DataType serialization
        let fields_json: Vec<serde_json::Value> = fields
            .iter()
            .map(|f| serde_json::to_value(f).unwrap())
            .collect();

        let table_schema_json = serde_json::json!({
            "version": 1,
            "id": schema_id,
            "fields": fields_json,
            "highestFieldId": highest_field_id,
            "partitionKeys": self.partition_keys,
            "primaryKeys": self.primary_keys,
            "options": self.options,
            "comment": self.comment,
            "timeMillis": time_millis,
        });

        serde_json::from_value(table_schema_json).map_err(|e| Error::DataInvalid {
            message: format!("Failed to create TableSchema from Schema: {}", e),
            source: Some(Box::new(e)),
        })
    }
}

/// Builder for Schema.
#[derive(Debug, Default)]
pub struct SchemaBuilder {
    fields: Vec<(String, DataType, Option<String>)>,
    primary_keys: Vec<String>,
    partition_keys: Vec<String>,
    options: HashMap<String, String>,
    comment: Option<String>,
}

impl SchemaBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a column to the schema.
    pub fn column(mut self, name: impl Into<String>, data_type: DataType) -> Self {
        self.fields.push((name.into(), data_type, None));
        self
    }

    /// Add a column with description to the schema.
    pub fn column_with_description(
        mut self,
        name: impl Into<String>,
        data_type: DataType,
        description: impl Into<String>,
    ) -> Self {
        self.fields
            .push((name.into(), data_type, Some(description.into())));
        self
    }

    /// Set primary keys.
    pub fn primary_key(mut self, keys: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.primary_keys = keys.into_iter().map(|k| k.into()).collect();
        self
    }

    /// Set partition keys.
    pub fn partition_keys(mut self, keys: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.partition_keys = keys.into_iter().map(|k| k.into()).collect();
        self
    }

    /// Set a table option.
    pub fn option(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.options.insert(key.into(), value.into());
        self
    }

    /// Set table comment.
    pub fn comment(mut self, comment: impl Into<String>) -> Self {
        self.comment = Some(comment.into());
        self
    }

    /// Build the Schema.
    pub fn build(self) -> Schema {
        Schema {
            fields: self.fields,
            primary_keys: self.primary_keys,
            partition_keys: self.partition_keys,
            options: self.options,
            comment: self.comment,
        }
    }
}
