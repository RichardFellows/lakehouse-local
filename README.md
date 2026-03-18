# lakehouse-local

A local development stack for testing **Airflow → dbt → Spark** pipelines with **Iceberg** tables and **Nessie** catalog, using **LocalStack** for S3-compatible storage.

All containers use **RHEL 8 (UBI)** base images and JARs are resolved via **Maven** at build time.

## Architecture

```
┌──────────────────────────────────────────────────────┐
│                    Airflow                            │
│                 (Orchestration)                       │
│                                                      │
│  ┌──────┐  ┌──────────┐  ┌──────────┐  ┌─────────┐ │
│  │ seed │→ │ staging  │→ │  marts   │→ │  test   │ │
│  └──────┘  └──────────┘  └──────────┘  └─────────┘ │
│       ↓          ↓             ↓            ↓        │
│                    dbt (SQL)                          │
└──────────────────────┬───────────────────────────────┘
                       │ Thrift/JDBC
                       ▼
              ┌─────────────────┐
              │   Spark Thrift  │
              │   Server        │
              │  (Compute)      │
              └────────┬────────┘
                       │
              ┌────────┴────────┐
              │                 │
     ┌────────▼──────┐  ┌──────▼───────┐
     │    Nessie     │  │  LocalStack  │
     │  (Catalog)    │  │    (S3)      │
     │  Git-like     │  │  Object      │
     │  branching    │  │  Storage     │
     └───────────────┘  └──────────────┘
```

## Prerequisites

- Docker & Docker Compose
- ~8GB RAM allocated to Docker
- ~5GB disk for images

## Quick Start

```bash
# Build and start everything
make up

# Wait ~60s for Spark Thrift to become healthy, then:
# 1. Open Airflow UI at http://localhost:8080
# 2. Find the "lakehouse_dbt_spark" DAG
# 3. Trigger it manually (play button)

# Or run dbt directly:
make dbt-run

# Interactive Spark SQL:
make spark-sql

# Check what's in Nessie:
make nessie-contents

# Check S3 buckets:
make s3-list

# Tear down:
make down

# Tear down + remove volumes:
make clean
```

## Components

| Service | Image | Port | Purpose |
|---------|-------|------|---------|
| LocalStack | `localstack/localstack` | 4566 | S3-compatible object storage |
| Nessie | `ghcr.io/projectnessie/nessie` | 19120 | Iceberg catalog with Git-like branching |
| Spark | Custom (UBI8 + Spark 3.5) | 7077, 8081, 10000 | Compute engine + Thrift Server |
| Airflow | Custom (UBI8 + Airflow 2.10) | 8080 | DAG orchestration |

## Stack Details

### Spark (UBI8)

- **Base:** `redhat/ubi8` with Java 17 + Python 3.11
- **Spark:** 3.5.4 with Hadoop 3
- **JARs:** Resolved via Maven at build time (see `spark/pom.xml`):
  - `iceberg-spark-runtime` — Iceberg table format support
  - `iceberg-aws-bundle` — S3FileIO for reading/writing Iceberg data
  - `nessie-spark-extensions` — Nessie catalog + SQL extensions
  - `hadoop-aws` — S3A filesystem implementation
  - `aws-sdk-v2-bundle` — AWS SDK for S3 operations
- **Thrift Server:** Exposes Spark SQL over JDBC on port 10000

### Airflow (UBI8)

- **Base:** `redhat/ubi8` with Python 3.11
- **Airflow:** 2.10.4 with LocalExecutor (SQLite)
- **dbt:** `dbt-spark[PyHive]` for Spark Thrift connectivity
- **Cosmos:** `astronomer-cosmos` (optional — for per-model Airflow tasks)

### dbt Project

Simple customer/orders model:
- **Seeds:** `customers.csv`, `orders.csv`
- **Staging:** Views over seed data with type casting
- **Marts:** `customer_orders` — aggregated customer metrics with tiering
- **Tests:** Unique, not-null, referential integrity

### Nessie

Zero-config — runs standalone with in-memory storage (fine for local dev). Provides:
- Git-like branch/merge for data
- Iceberg catalog API
- REST API on port 19120

### LocalStack

Provides S3-compatible storage. Buckets (`warehouse`, `raw-data`) are auto-created via init script.

## Production Mapping

| Local | Production |
|-------|-----------|
| LocalStack | AWS S3 |
| Nessie (standalone) | Nessie (persistent, e.g. on RDS) |
| Spark Thrift (local[*]) | Spark cluster (YARN/K8s) |
| Airflow standalone | Airflow (Celery/K8s executor) |
| dbt-spark | dbt-spark / dbt-databricks |

To target production, swap the dbt profile:

```yaml
lakehouse_local:
  target: prod
  outputs:
    prod:
      type: spark
      method: thrift
      host: spark-thrift.internal
      port: 10000
      schema: production
      threads: 8
```

## Nessie Branching (Bonus)

Connect via `make spark-sql` and try:

```sql
-- Create a development branch
CREATE BRANCH dev IN nessie;

-- Switch to it
USE REFERENCE dev IN nessie;

-- Make changes (they're isolated from main)
INSERT INTO nessie.db.customer_orders VALUES (...);

-- Switch back and merge when ready
USE REFERENCE main IN nessie;
MERGE BRANCH dev INTO main IN nessie;
```

## Troubleshooting

**Spark Thrift not starting:**
- Check logs: `docker compose logs spark`
- Ensure LocalStack and Nessie are up first
- Maven download failures usually mean network issues during build

**dbt can't connect to Spark:**
- Verify Thrift is healthy: `docker compose ps` (should show "healthy")
- Test manually: `docker compose exec airflow python3 -c "from pyhive import hive; hive.connect('spark', 10000)"`

**Iceberg table creation fails:**
- Check Nessie is reachable: `curl http://localhost:19120/api/v2/config`
- Check S3 buckets exist: `make s3-list`

**Out of memory:**
- Increase Docker RAM allocation to 8GB+
- Reduce Spark memory in `entrypoint.sh` (driver/executor memory)

## License

MIT
