# Adding New Data Sources

Step-by-step guide to adding CSV data sources to the lakehouse-local dbt project.

## Overview

The dbt project follows a three-layer pattern:

```
Seeds (CSV) → Staging (views) → Marts (tables)
```

- **Seeds** — raw CSV files loaded by `dbt seed`
- **Staging** — thin views that clean/cast types (prefix: `stg_`)
- **Marts** — business-level aggregations and joins (the tables your notebooks query)

---

## Step 1: Add Your CSV File

Drop your CSV into `dbt_project/seeds/`:

```
dbt_project/seeds/products.csv
```

Example `products.csv`:
```csv
product_id,name,category,price,created_at
1,Widget A,electronics,29.99,2024-01-10
2,Widget B,electronics,49.99,2024-02-15
3,Gadget C,accessories,12.50,2024-03-20
4,Gadget D,accessories,8.99,2024-04-01
5,Thingamajig,other,199.99,2024-05-05
```

**Rules:**
- First row must be column headers
- Use consistent types per column (don't mix numbers and text)
- Dates should be `YYYY-MM-DD` format
- No trailing commas or empty rows

---

## Step 2: Create a Staging Model

Create `dbt_project/models/staging/stg_products.sql`:

```sql
{{ config(materialized='view') }}

select
    product_id,
    name,
    category,
    cast(price as decimal(10, 2)) as price,
    cast(created_at as date) as created_at
from {{ ref('products') }}
```

**Why staging?**
- Cast columns to the right types (CSV loads everything as strings in some engines)
- Rename columns if needed
- Filter bad data
- Provide a clean interface for downstream models

---

## Step 3: Add Tests (schema.yml)

Add your model to `dbt_project/models/staging/schema.yml`:

```yaml
version: 2

models:
  # ... existing models ...

  - name: stg_products
    description: Staged product catalog
    columns:
      - name: product_id
        description: Primary key
        tests:
          - unique
          - not_null
      - name: price
        tests:
          - not_null
      - name: category
        tests:
          - not_null
          - accepted_values:
              values: ['electronics', 'accessories', 'other']
```

---

## Step 4: Create a Mart (optional)

If you want to join the new data with existing tables, create a mart.

Example — `dbt_project/models/marts/order_products.sql`:

```sql
{{ config(materialized='table') }}

with orders as (
    select * from {{ ref('stg_orders') }}
),

products as (
    select * from {{ ref('stg_products') }}
)

select
    o.order_id,
    o.customer_id,
    o.order_date,
    p.name as product_name,
    p.category,
    o.amount,
    o.status
from orders o
left join products p on o.product_id = p.product_id
```

> **Note:** The example orders CSV doesn't have a `product_id` column — you'd
> need to add one to `orders.csv` or create a separate `order_items.csv` seed
> to join orders to products.

---

## Step 5: Run It

### Via the PowerShell helper (on Windows):
```powershell
# Seed the new CSV
.\lakehouse.ps1 dbt-seed

# Run models
.\lakehouse.ps1 dbt-run

# Run tests
.\lakehouse.ps1 dbt-test
```

### Via Docker directly:
```bash
# DuckDB target (default)
docker exec lakehouse-local-airflow-1 bash -c \
  "cd /opt/dbt && dbt seed --profiles-dir /opt/dbt"

docker exec lakehouse-local-airflow-1 bash -c \
  "cd /opt/dbt && dbt run --profiles-dir /opt/dbt"

# Spark target
docker exec lakehouse-local-airflow-1 bash -c \
  "cd /opt/dbt && DBT_TARGET=spark dbt seed --profiles-dir /opt/dbt && \
   DBT_TARGET=spark dbt run --profiles-dir /opt/dbt"
```

### Via Airflow:
Trigger the `lakehouse_duckdb` or `lakehouse_spark` DAG from the UI at `http://localhost:8082`.

---

## Step 6: Query in Notebooks

Once the models are built, they're immediately available in the notebooks:

| Notebook | How to query |
|----------|-------------|
| `explore.py` | Automatically picks up new tables in `db` schema |
| `duckdb_local.py` | `SELECT * FROM stg_products` |
| `spark_thrift.py` | `SELECT * FROM stg_products` (after Spark DAG) |
| `pyiceberg_direct.py` | `catalog.load_table("db.stg_products")` (Spark tables only) |
| `duckdb_iceberg.py` | `iceberg_query("db.stg_products", "SELECT * FROM {table}")` (Spark tables only) |

> **Important:** PyIceberg and DuckDB+Iceberg only see tables created by the **Spark** DAG
> (Iceberg format). DuckDB-only models are in the local `.duckdb` file.

---

## File Layout After Adding a Source

```
dbt_project/
├── seeds/
│   ├── customers.csv          # existing
│   ├── orders.csv             # existing
│   └── products.csv           # ← new
├── models/
│   ├── staging/
│   │   ├── stg_customers.sql  # existing
│   │   ├── stg_orders.sql     # existing
│   │   ├── stg_products.sql   # ← new
│   │   └── schema.yml         # ← updated
│   └── marts/
│       ├── customer_orders.sql # existing
│       └── order_products.sql  # ← new (optional)
├── profiles.yml
└── dbt_project.yml
```

---

## Tips

- **Large CSVs**: dbt seed is designed for small reference data (< 10K rows). For larger files, load them into S3 via LocalStack and create external tables instead.

- **Multiple related CSVs**: Add a `_schema.yml` per directory if you prefer, or keep everything in one `schema.yml` per layer.

- **Incremental models**: For append-only data, use `{{ config(materialized='incremental') }}` in your mart models. This only processes new rows on subsequent runs.

- **Custom seed config**: Override seed behaviour in `dbt_project.yml`:
  ```yaml
  seeds:
    lakehouse_local:
      products:
        +column_types:
          price: decimal(10,2)
          created_at: date
  ```

- **Testing the full pipeline**: Run `dbt build` instead of `seed` + `run` + `test` separately — it does all three in dependency order.

---

## Production Pattern: File Drops → S3 → Iceberg

For feeds that arrive as file drops (SFTP, network share, etc.), use the `file_ingest` DAG
instead of seeds. See `feeds/README.md` for details.

```
feeds/incoming/products.csv  →  S3 (raw-data)  →  Iceberg (nessie.raw.products)  →  dbt marts
```

To test:
```bash
cp feeds/sample_products.csv feeds/incoming/products.csv
# Wait for DAG or trigger manually in Airflow UI
```
