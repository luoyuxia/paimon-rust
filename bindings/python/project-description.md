<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# PyPaimon Core

This project is used to build a paimon-rust powered core for pypaimon, and intended for use only by pypaimon.

Install via PyPI:

```
pip install pypaimon-core
```

## Query Paimon Tables with DataFusion

`pypaimon-core` provides a built-in DataFusion integration through `PaimonContext`.
It creates a Paimon catalog from an options dict and registers it into a DataFusion session,
so you can query any Paimon table with SQL directly.

```python
import pyarrow as pa
from pypaimon_core.datafusion import PaimonContext

# Create a context with a filesystem catalog
ctx = PaimonContext(catalog_options={
    "warehouse": "/path/to/warehouse",
})

# Query tables via SQL (catalog.database.table)
df = ctx.sql("SELECT * FROM paimon.`default`.my_table LIMIT 10")
df.show()

# Collect results as PyArrow RecordBatches
batches = df.collect()
```

### REST Catalog

```python
ctx = PaimonContext(catalog_options={
    "metastore": "rest",
    "uri": "http://localhost:8080",
    "warehouse": "my_warehouse",
})
```

### Time Travel

Time travel is supported via standard SQL `FOR SYSTEM_TIME AS OF` syntax:

```python
# Travel to a specific snapshot id
df = ctx.sql("SELECT * FROM paimon.`default`.my_table FOR SYSTEM_TIME AS OF 1")

# Travel to a specific timestamp
df = ctx.sql("SELECT * FROM paimon.`default`.my_table FOR SYSTEM_TIME AS OF '2024-01-01 00:00:00'")

# Travel to a tag
df = ctx.sql("SELECT * FROM paimon.`default`.my_table FOR SYSTEM_TIME AS OF 'my_tag'")
```

### DataFusion Configuration

`PaimonContext` uses BigQuery SQL dialect by default to enable time travel syntax.
You can pass additional DataFusion session configuration via the `datafusion_config` parameter:

```python
ctx = PaimonContext(
    catalog_options={"warehouse": "/path/to/warehouse"},
    datafusion_config={
        "datafusion.execution.target_partitions": "8",
        "datafusion.execution.batch_size": "4096",
    },
)
```

To override the default dialect:

```python
ctx = PaimonContext(
    catalog_options={"warehouse": "/path/to/warehouse"},
    datafusion_config={
        "datafusion.sql_parser.dialect": "Generic",
    },
)
```
