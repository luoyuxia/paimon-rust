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

//! ReadBuilder and TableRead for table read API.
//!
//! Reference: [pypaimon.read.read_builder.ReadBuilder](https://github.com/apache/paimon/blob/master/paimon-python/pypaimon/read/read_builder.py)
//! and [pypaimon.table.file_store_table.FileStoreTable.new_read_builder](https://github.com/apache/paimon/blob/master/paimon-python/pypaimon/table/file_store_table.py).

use super::{ArrowRecordBatchStream, Table, TableScan};
use crate::arrow::ArrowReaderBuilder;
use crate::spec::DataField;
use crate::DataSplit;

/// Builder for table scan and table read (with_projection, new_scan, new_read).
///
/// Reference: [pypaimon.read.read_builder.ReadBuilder](https://github.com/apache/paimon/blob/master/paimon-python/pypaimon/read/read_builder.py)
#[derive(Debug, Clone)]
pub struct ReadBuilder<'a> {
    table: &'a Table,
    projection: Option<Vec<String>>,
}

impl<'a> ReadBuilder<'a> {
    pub(crate) fn new(table: &'a Table) -> Self {
        Self {
            table,
            projection: None,
        }
    }

    /// Set projection (column names to read). If not set, all columns are read.
    pub fn with_projection(mut self, projection: Vec<String>) -> Self {
        self.projection = Some(projection);
        self
    }

    /// Create a table scan. Call [TableScan::plan] to get splits.
    pub fn new_scan(self) -> TableScan<'a> {
        TableScan::new(self.table)
    }

    /// Create a table read for consuming splits (e.g. from a scan plan).
    pub fn new_read(self) -> TableRead<'a> {
        let read_type = self.read_type();
        TableRead {
            table: self.table,
            read_type,
        }
    }

    /// Fields to read (projected or full schema). Used by [TableRead] and predicate builder.
    fn read_type(&self) -> Vec<DataField> {
        let fields = self.table.schema().fields();
        match &self.projection {
            None => fields.to_vec(),
            Some(proj) => {
                let name_to_field: std::collections::HashMap<_, _> = fields
                    .iter()
                    .map(|f| (f.name().to_string(), f.clone()))
                    .collect();
                proj.iter()
                    .filter_map(|name| name_to_field.get(name).cloned())
                    .collect()
            }
        }
    }
}

/// Table read: reads data from splits (e.g. produced by [TableScan::plan]).
///
/// Reference: [pypaimon.read.table_read.TableRead](https://github.com/apache/paimon/blob/master/paimon-python/pypaimon/read/table_read.py)
#[derive(Debug, Clone)]
pub struct TableRead<'a> {
    table: &'a Table,
    read_type: Vec<DataField>,
}

impl<'a> TableRead<'a> {
    /// Schema (fields) that this read will produce.
    pub fn read_type(&self) -> &[DataField] {
        &self.read_type
    }

    /// Table for this read.
    pub fn table(&self) -> &Table {
        self.table
    }

    pub fn to_arrow(&self, data_splits: &[DataSplit]) -> crate::Result<ArrowRecordBatchStream> {
        let arrow_reader_builder = ArrowReaderBuilder::new(self.table.file_io.clone()).build();
        arrow_reader_builder.read(data_splits)
    }
}
