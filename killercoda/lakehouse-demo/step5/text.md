# Query results with DuckDB

Now the pipeline has run, let's query the results directly using DuckDB's CLI.

## Install DuckDB CLI

```bash
curl -fsSL https://github.com/duckdb/duckdb/releases/download/v1.2.1/duckdb_cli-linux-amd64.zip -o /tmp/duckdb.zip
unzip -o /tmp/duckdb.zip -d /usr/local/bin/
chmod +x /usr/local/bin/duckdb
```

## Copy the database locally

```bash
cd /root/lakehouse-local
docker compose cp airflow:/opt/dbt/lakehouse.duckdb ./lakehouse.duckdb
```

## Explore the data

List all tables and views:

```bash
duckdb lakehouse.duckdb "SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = 'main' ORDER BY table_type, table_name;"
```

## Query the mart

The `customer_orders` table has our aggregated results:

```bash
duckdb lakehouse.duckdb "SELECT first_name, last_name, total_orders, total_revenue, customer_tier FROM customer_orders ORDER BY total_revenue DESC;"
```

You should see Alice at the top (3 orders, high tier) and Eve at the bottom (no orders, low tier).

## Trace a customer through the layers

Follow Alice from seed → staging → mart:

```bash
echo "=== Seed (raw) ===" 
duckdb lakehouse.duckdb "SELECT * FROM customers WHERE customer_id = 1;"

echo ""
echo "=== Staging (typed) ==="
duckdb lakehouse.duckdb "SELECT * FROM stg_customers WHERE customer_id = 1;"

echo ""
echo "=== Mart (aggregated) ==="
duckdb lakehouse.duckdb "SELECT * FROM customer_orders WHERE customer_id = 1;"
```

This is the classic dbt pattern: raw data → cleaned staging → business-ready marts.
