# lakehouse-local

A local development stack for testing **Airflow → dbt** pipelines with **Iceberg** tables and **Nessie** catalog, using **LocalStack** for S3-compatible storage.

Supports two engine modes:
- **DuckDB** (default) — lightweight, no JVM, starts in seconds
- **Spark** — full production-parity stack with Thrift Server + Nessie

All containers use **RHEL 8 (UBI)** base images. Spark JARs are resolved via **Maven** at build time.

## Architecture

### DuckDB Mode (default)
```
┌─────────────────────┐
│      Airflow        │
│   ┌─────────────┐   │
│   │  dbt-duckdb │   │
│   │  (in-process)│   │
│   └──────┬──────┘   │
│          │           │
│     DuckDB Engine    │
└──────────┬───────────┘
           │ httpfs
    ┌──────▼───────┐
    │  LocalStack  │
    │    (S3)      │
    └──────────────┘
```

### Spark Mode
```
┌──────────────────────────────────────────────────────┐
│                    Airflow                            │
│  ┌──────┐  ┌──────────┐  ┌──────────┐  ┌─────────┐ │
│  │ seed │→ │ staging  │→ │  marts   │→ │  test   │ │
│  └──────┘  └──────────┘  └──────────┘  └─────────┘ │
│                    dbt (SQL)                          │
└──────────────────────┬───────────────────────────────┘
                       │ Thrift/JDBC
              ┌────────▼────────┐
              │   Spark Thrift  │
              └────────┬────────┘
              ┌────────┴────────┐
     ┌────────▼──────┐  ┌──────▼───────┐
     │    Nessie     │  │  LocalStack  │
     │  (Catalog)    │  │    (S3)      │
     └───────────────┘  └──────────────┘
```

## Prerequisites

- Windows 10/11 with Docker Desktop
- PowerShell 5.1+ (built-in) or PowerShell 7+
- Docker Desktop RAM: 2-4GB (DuckDB) or 8GB+ (Spark)
- ~2GB disk (DuckDB) or ~5GB disk (Spark) for images

## Quick Start

```powershell
# DuckDB mode (default — fast, lightweight)
.\lakehouse.ps1 up

# Or full Spark stack
.\lakehouse.ps1 up -Engine spark
```

Then:
1. Open Airflow UI at http://localhost:8080
2. Find the `lakehouse_dbt_spark` DAG
3. Trigger it manually (play button)

```powershell
# Run dbt directly
.\lakehouse.ps1 dbt-run
.\lakehouse.ps1 dbt-test
.\lakehouse.ps1 dbt-debug

# Interactive Spark SQL (spark mode only)
.\lakehouse.ps1 spark-sql -Engine spark

# Check Nessie catalog (spark mode only)
.\lakehouse.ps1 nessie-contents -Engine spark

# Check S3 buckets
.\lakehouse.ps1 s3-list

# Tear down
.\lakehouse.ps1 down

# Tear down + remove volumes
.\lakehouse.ps1 clean

# See all commands
.\lakehouse.ps1 help
```

## Engine Comparison

| | DuckDB | Spark |
|---|---|---|
| **Startup** | ~10s | ~60s |
| **Memory** | 2-4GB | 8GB+ |
| **Containers** | 2 (Airflow + LocalStack) | 4 (+Spark + Nessie) |
| **JVM required** | No | Yes |
| **Build time** | Seconds | Minutes (Maven) |
| **Production parity** | Different engine | Matches prod Spark |
| **Best for** | Fast iteration, CI | Integration testing |

The **same dbt models** run on both engines — the `profiles.yml` uses an environment variable to select the target:

```yaml
target: "{{ env_var('DBT_TARGET', 'duckdb') }}"
```

## Components

| Service | Image | Port | Profile |
|---------|-------|------|---------|
| LocalStack | `localstack/localstack` | 4566 | both |
| Airflow (DuckDB) | Custom (UBI8 + dbt-duckdb) | 8080 | `duckdb` |
| Airflow (Spark) | Custom (UBI8 + dbt-spark) | 8080 | `spark` |
| Spark | Custom (UBI8 + Spark 3.5) | 7077, 8081, 10000 | `spark` |
| Nessie | `ghcr.io/projectnessie/nessie` | 19120 | `spark` |

## Stack Details

### Spark Image (UBI8)

- **Base:** `redhat/ubi8` with Java 17 + Python 3.11
- **Spark:** 3.5.4 with Hadoop 3
- **JARs:** Resolved via Maven at build time (see `spark/pom.xml`):
  - `iceberg-spark-runtime` — Iceberg table format
  - `iceberg-aws-bundle` — S3FileIO
  - `nessie-spark-extensions` — Nessie catalog + SQL extensions
  - `hadoop-aws` — S3A filesystem
  - `aws-sdk-v2-bundle` — AWS SDK for S3
- **Thrift Server:** Exposes Spark SQL over JDBC on port 10000

### Airflow Images (UBI8)

- **DuckDB variant:** Python 3.11 + `dbt-duckdb` + `astronomer-cosmos` (~700MB)
- **Spark variant:** Python 3.11 + Java 17 + `dbt-spark[PyHive]` + `astronomer-cosmos` (~1.2GB)

### dbt Project

Simple customer/orders model demonstrating the stack:
- **Seeds:** `customers.csv`, `orders.csv`
- **Staging:** Views over seed data with type casting
- **Marts:** `customer_orders` — aggregated customer metrics with tiering
- **Tests:** Unique, not-null, referential integrity

### Nessie (Spark mode only)

Zero-config standalone with in-memory storage. Provides Git-like branch/merge for Iceberg data.

### LocalStack

S3-compatible storage. Buckets (`warehouse`, `raw-data`) auto-created via init script.

## Production Mapping

| Local | Production |
|-------|-----------|
| LocalStack | AWS S3 |
| DuckDB / Spark Thrift | Spark cluster (YARN/K8s) |
| Nessie (standalone) | Nessie (persistent backend) |
| Airflow standalone | Airflow (Celery/K8s executor) |

## Nessie Branching (Spark mode)

Connect via `.\lakehouse.ps1 spark-sql -Engine spark` and try:

```sql
CREATE BRANCH dev IN nessie;
USE REFERENCE dev IN nessie;
-- Make changes isolated from main
MERGE BRANCH dev INTO main IN nessie;
```

## Troubleshooting

**PowerShell execution policy:**
```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

**Spark Thrift not starting:**
- Check logs: `docker compose --profile spark logs spark`
- Maven download failures = network issues during build

**dbt can't connect:**
- DuckDB: should work immediately (in-process)
- Spark: verify Thrift is healthy — `docker compose ps`

**Iceberg table creation fails (Spark mode):**
- Nessie reachable? `Invoke-RestMethod http://localhost:19120/api/v2/config`
- S3 buckets exist? `.\lakehouse.ps1 s3-list`

**Out of memory:**
- Docker Desktop → Settings → Resources → increase RAM
- Spark: reduce memory in `spark/entrypoint.sh`

## License

MIT
