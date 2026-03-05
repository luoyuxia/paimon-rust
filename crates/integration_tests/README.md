# Paimon Integration Tests

- **Catalog/table tests** use a temporary directory as warehouse (no external services).
- **Read tests** use tables written by Spark + Paimon in Docker; they run only when `PAIMON_TEST_WAREHOUSE` is set.

## Running

### Catalog-only tests (no Docker)

From the repository root:

```bash
cargo test -p paimon-integration-tests
```

This runs all catalog/table lifecycle tests. Read-from-Spark tests are skipped when `PAIMON_TEST_WAREHOUSE` is unset.

### Read tests (Spark-provisioned data)

Because paimon-rust does not yet support writing table data, read tests rely on Spark + Paimon to write tables to a local warehouse:

1. **Provision data** (run once):

   ```bash
   docker compose -f dev/docker-compose.yaml run --rm spark-paimon
   ```

   This writes Paimon tables into `dev/warehouse` (created if missing).

2. **Run read integration tests**:

   ```bash
   PAIMON_TEST_WAREHOUSE=./dev/warehouse cargo test -p paimon-integration-tests read_from_spark
   ```

To re-provision: remove `dev/warehouse` and run step 1 again.

## Structure

- `src/lib.rs` – Fixtures: `create_test_catalog()` (temp dir), `spark_warehouse_path()` (env), `create_test_catalog_at(path)`.
- `tests/common/` – Shared helpers (e.g. `test_schema()`, `minimal_schema()`).
- `tests/catalog_and_tables.rs` – Database and table lifecycle tests (create, list, get, drop, rename, ignore_if_exists, cascade).
- `tests/read_from_spark.rs` – Read tests (schema, snapshot) on tables written by Spark; skipped unless `PAIMON_TEST_WAREHOUSE` is set.

## Reference

Inspired by [apache/iceberg-rust integration tests](https://github.com/apache/iceberg-rust/tree/main/crates/integration_tests) and [PR #766](https://github.com/apache/iceberg-rust/pull/766) (Spark for integration tests). Here, catalog tests use only the local file system; read tests use a Spark + Paimon container to write data, then paimon-rust reads it.
