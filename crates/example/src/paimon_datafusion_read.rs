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

use std::sync::Arc;

use datafusion::prelude::SessionContext;
use paimon::catalog::{Catalog, FileSystemCatalog, Identifier};
use paimon::io::FileIOBuilder;
use paimon_datafusion::PaimonTableProvider;

/// Example: read a Paimon table via DataFusion.
/// Set env PAIMON_WAREHOUSE to your warehouse path (default: ./paimon-warehouse).
#[tokio::main]
async fn main() -> paimon::Result<()> {
    let warehouse_path = "/Users/yuxia/Projects/paimon/paimon-rust-demo/warehouse";

    let file_io = FileIOBuilder::new("file").build()?;
    let catalog = FileSystemCatalog::new(file_io, warehouse_path);

    let table_id = Identifier::new("default", "T");
    let table = catalog.get_table(&table_id).await?;

    let provider = PaimonTableProvider::try_new(table).map_err(|e| {
        paimon::Error::DataInvalid {
            message: e.to_string(),
            source: None,
        }
    })?;

    let ctx = SessionContext::new();
    ctx.register_table("my_table", Arc::new(provider))
        .unwrap();

    let df = ctx.sql("SELECT * FROM my_table").await.map_err(|e| {
        paimon::Error::DataInvalid {
            message: e.to_string(),
            source: None,
        }
    })?;
    df.show().await.map_err(|e| paimon::Error::DataInvalid {
        message: e.to_string(),
        source: None,
    })?;

    Ok(())
}
