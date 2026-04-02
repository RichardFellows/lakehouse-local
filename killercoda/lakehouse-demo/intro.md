# Local Lakehouse: Airflow + dbt + Iceberg + Nessie

In this scenario you'll build a **local data lakehouse** from scratch using Docker Compose.

## What you'll learn

- How **Airflow** orchestrates dbt pipelines as DAGs
- How **dbt** transforms raw seed data through staging → marts layers
- How **DuckDB** provides a lightweight, JVM-free compute engine
- How **Spark + Iceberg + Nessie** provides production-parity compute with Git-like data versioning
- How the **same dbt models** run on both engines without code changes

## Architecture

```
┌──────────────────────────────────────────────────┐
│                    Airflow                        │
│    seed → staging → marts → test                 │
│                 dbt (SQL)                         │
└──────────────────┬───────────────────────────────┘
                   │
          ┌────────┴────────┐
    ┌─────▼─────┐    ┌─────▼──────┐
    │  DuckDB   │    │   Spark    │
    │(in-process)│    │  Thrift   │
    └─────┬─────┘    └─────┬─────┘
          │           ┌────┴────┐
          │     ┌─────▼───┐ ┌──▼──────┐
          │     │ Nessie  │ │   S3    │
          │     │(catalog)│ │(LocalSt)│
          └─────┼─────────┘ └─────────┘
                └───────────────┘
```

## Components

| Service | Role |
|---------|------|
| **Airflow** | Workflow orchestration |
| **dbt** | SQL-based data transforms |
| **DuckDB** | Lightweight analytical engine |
| **Spark** | Production-parity compute |
| **Nessie** | Iceberg catalog with Git-like branching |
| **LocalStack** | S3-compatible object storage |

Let's get started!
