# Dev: Spark + Paimon for integration tests

This directory provides a **Spark image with the Paimon connector** that writes tables to a local directory. paimon-rust integration tests can then read those tables to verify read support (schema, snapshots, etc.), since paimon-rust does not yet implement writing.

## Quick start

**Option A – Docker Compose (if your Docker has buildx):**

```bash
# From repo root: write test tables into dev/warehouse
docker compose -f dev/docker-compose.yaml run --rm spark-paimon
```

**Option B – Plain Docker (if you see “docker-buildx: no such file or directory”):**

```bash
# From dev/ directory: build image and run provision
cd dev && make provision
# Or from repo root:
make -C dev provision
```

Then run read integration tests:

```bash
PAIMON_TEST_WAREHOUSE=./dev/warehouse cargo test -p paimon-integration-tests read_from_spark
```

## Contents

- **docker-compose.yaml** – Defines the `spark-paimon` service; mounts `./warehouse` as the Paimon warehouse in the container.
- **spark/Dockerfile** – Builds an image based on `apache/spark:3.5.3` and adds the Paimon Spark 3.5 JAR; configures the Paimon filesystem catalog with `warehouse=file:/tmp/paimon-warehouse`.
- **spark/spark-defaults.conf** – Spark config for the Paimon catalog and warehouse path.
- **spark/provision.py** – PySpark script that creates tables `simple` and `partitioned` and inserts rows. Run via `spark-submit` when the container starts.

The warehouse directory (`dev/warehouse`) is gitignored. To regenerate data, remove it and run the provision step again.

## Troubleshooting

**`fork/exec ... docker-buildx: no such file or directory`**  
Docker is trying to use the buildx plugin, which is missing or broken. Use **Option B** above: from the repo root run `make -C dev provision`. The Makefile uses `DOCKER_BUILDKIT=0` so `docker build` uses the legacy builder and does not require buildx. Alternatively, install or repair the Docker Buildx plugin for your Docker Desktop version.
