# Explore the dbt project

Before running the pipeline, let's understand what dbt will do.

## Seed data

dbt uses CSV files as seed data — these simulate raw data that would normally come from source systems:

```bash
cd /root/lakehouse-local
echo "=== Customers ===" && cat dbt_project/seeds/customers.csv
echo ""
echo "=== Orders ===" && cat dbt_project/seeds/orders.csv
```

We have 5 customers and 8 orders — small enough to trace through the transforms.

## Staging models

Staging models clean and type-cast the raw data:

```bash
cat dbt_project/models/staging/stg_customers.sql
```

```bash
cat dbt_project/models/staging/stg_orders.sql
```

These are materialised as **views** — lightweight, no data duplication.

## Mart models

The mart layer aggregates data for business use:

```bash
cat dbt_project/models/marts/customer_orders.sql
```

This creates a **table** with:
- Total orders per customer
- Total revenue (completed orders only)
- First and last order dates
- A customer tier (low/medium/high) based on order count

## DAG definition

Airflow orchestrates these steps in order:

```bash
cat dags/lakehouse_duckdb.py
```

The pipeline: `debug → seed → staging → marts → test` — a standard dbt workflow wrapped in Airflow tasks.
