# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import os

import pyarrow as pa

from pypaimon_core.datafusion import PaimonContext

WAREHOUSE = os.environ.get("PAIMON_TEST_WAREHOUSE", "/tmp/paimon-warehouse")


def extract_rows(batches):
    table = pa.Table.from_batches(batches)
    return sorted(zip(table["id"].to_pylist(), table["name"].to_pylist()))


def test_time_travel():
    ctx = PaimonContext(catalog_options={"warehouse": WAREHOUSE})

    snapshot1_expected = [(1, "alice"), (2, "bob")]
    snapshot2_expected = [(1, "alice"), (2, "bob"), (3, "carol"), (4, "dave")]

    # Time travel by snapshot id
    df = ctx.sql(
        "SELECT id, name FROM paimon.`default`.time_travel_table "
        "FOR SYSTEM_TIME AS OF 1"
    )
    assert extract_rows(df.collect()) == snapshot1_expected

    df = ctx.sql(
        "SELECT id, name FROM paimon.`default`.time_travel_table "
        "FOR SYSTEM_TIME AS OF 2"
    )
    assert extract_rows(df.collect()) == snapshot2_expected

    # Time travel by tag name
    df = ctx.sql(
        "SELECT id, name FROM paimon.`default`.time_travel_table "
        "FOR SYSTEM_TIME AS OF 'snapshot1'"
    )
    assert extract_rows(df.collect()) == snapshot1_expected

    df = ctx.sql(
        "SELECT id, name FROM paimon.`default`.time_travel_table "
        "FOR SYSTEM_TIME AS OF 'snapshot2'"
    )
    assert extract_rows(df.collect()) == snapshot2_expected

    # Time travel by timestamp (use a far-future timestamp to get all rows)
    df = ctx.sql(
        "SELECT id, name FROM paimon.`default`.time_travel_table "
        "FOR SYSTEM_TIME AS OF '2999-12-31 23:59:59'"
    )
    assert extract_rows(df.collect()) == snapshot2_expected
