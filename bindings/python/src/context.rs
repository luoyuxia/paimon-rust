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

use std::collections::HashMap;
use std::sync::Arc;

use arrow::pyarrow::ToPyArrow;
use datafusion::dataframe::DataFrame;
use datafusion::prelude::{SessionConfig, SessionContext};
use paimon::{CatalogFactory, Options};
use paimon_datafusion::{PaimonCatalogProvider, PaimonRelationPlanner};
use pyo3::prelude::*;

use crate::error::{df_to_py_err, to_py_err};
use crate::runtime::runtime;

/// A DataFusion session context with a Paimon catalog registered.
///
/// Creates a Paimon catalog from the given options and registers it
/// with a DataFusion session, enabling SQL queries over Paimon tables.
#[pyclass(name = "PaimonContext")]
pub struct PaimonContext {
    ctx: Arc<SessionContext>,
}

#[pymethods]
impl PaimonContext {
    /// Create a new PaimonContext.
    ///
    /// Args:
    ///     catalog_options: Options for creating the Paimon catalog (e.g. {"warehouse": "/path"}).
    ///     catalog_name: Name to register the catalog under (default: "paimon").
    ///     datafusion_config: Optional DataFusion session config overrides.
    ///         Defaults include BigQuery dialect for time travel support.
    #[new]
    #[pyo3(signature = (catalog_options, catalog_name=None, datafusion_config=None))]
    fn new(
        catalog_options: HashMap<String, String>,
        catalog_name: Option<String>,
        datafusion_config: Option<HashMap<String, String>>,
    ) -> PyResult<Self> {
        let rt = runtime();
        let catalog_name = catalog_name.unwrap_or_else(|| "paimon".to_string());

        let ctx = rt.block_on(async {
            // Create Paimon catalog via CatalogFactory
            let options = Options::from_map(catalog_options);
            let catalog = CatalogFactory::create(options).await.map_err(to_py_err)?;

            // Default: BigQuery dialect for time travel (FOR SYSTEM_TIME AS OF)
            let mut config =
                SessionConfig::new().set_str("datafusion.sql_parser.dialect", "BigQuery");

            // Apply user overrides (can override dialect if needed)
            if let Some(overrides) = datafusion_config {
                for (key, value) in overrides {
                    config = config.set_str(&key, &value);
                }
            }

            let ctx = SessionContext::new_with_config(config);

            // Register the Paimon catalog
            ctx.register_catalog(
                &catalog_name,
                Arc::new(PaimonCatalogProvider::new(catalog)),
            );

            // Register relation planner for time travel (FOR SYSTEM_TIME AS OF)
            ctx.register_relation_planner(Arc::new(PaimonRelationPlanner::new()))
                .map_err(df_to_py_err)?;

            Ok::<_, PyErr>(ctx)
        })?;

        Ok(Self {
            ctx: Arc::new(ctx),
        })
    }

    /// Execute a SQL query and return a DataFrame.
    fn sql(&self, py: Python, query: &str) -> PyResult<PyDataFrame> {
        let ctx = self.ctx.clone();
        let query = query.to_string();
        let df = py.detach(move || {
            let rt = runtime();
            rt.block_on(async { ctx.sql(&query).await.map_err(df_to_py_err) })
        })?;
        Ok(PyDataFrame { df })
    }
}

/// A DataFusion DataFrame result that can be collected as PyArrow RecordBatches.
#[pyclass(name = "DataFrame")]
pub struct PyDataFrame {
    df: DataFrame,
}

#[pymethods]
impl PyDataFrame {
    /// Collect all results as a list of PyArrow RecordBatches.
    fn collect<'py>(&self, py: Python<'py>) -> PyResult<Vec<Bound<'py, PyAny>>> {
        let df = self.df.clone();
        let batches = py.detach(move || {
            let rt = runtime();
            rt.block_on(async { df.collect().await.map_err(df_to_py_err) })
        })?;
        batches.iter().map(|batch| batch.to_pyarrow(py)).collect()
    }

    /// Print the results to stdout.
    fn show(&self, py: Python) -> PyResult<()> {
        let df = self.df.clone();
        py.detach(move || {
            let rt = runtime();
            rt.block_on(async { df.show().await.map_err(df_to_py_err) })
        })
    }
}

pub fn register_module(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let this = PyModule::new(py, "datafusion")?;
    this.add_class::<PaimonContext>()?;
    this.add_class::<PyDataFrame>()?;
    m.add_submodule(&this)?;
    py.import("sys")?
        .getattr("modules")?
        .set_item("pypaimon_core.datafusion", this)?;
    Ok(())
}
