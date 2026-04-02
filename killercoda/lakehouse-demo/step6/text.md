# Add the Spark engine and compare

Now for the powerful bit — we'll bring up the full Spark stack and run the **same dbt models** on a production-parity engine.

## Start Spark + Nessie

```bash
cd /root/lakehouse-local
docker compose up -d nessie spark
```

The Spark image builds from source with Maven dependency resolution — this takes a few minutes on first run.

```bash
echo "⏳ Waiting for Spark Thrift Server..."
until docker compose exec spark bash -c "echo > /dev/tcp/localhost/10000" 2>/dev/null; do sleep 5; done
echo "✅ Spark Thrift Server ready"
```

## Verify Nessie catalog

```bash
curl -s http://localhost:19120/api/v2/config | python3 -m json.tool
```

Nessie provides Git-like versioning for your Iceberg tables — branches, commits, merges.

## Run dbt on Spark

The **same models**, now targeting Spark instead of DuckDB:

```bash
docker compose exec airflow bash -c "cd /opt/dbt && DBT_TARGET=spark dbt seed --profiles-dir /opt/dbt"
docker compose exec airflow bash -c "cd /opt/dbt && DBT_TARGET=spark dbt run --profiles-dir /opt/dbt"
docker compose exec airflow bash -c "cd /opt/dbt && DBT_TARGET=spark dbt test --profiles-dir /opt/dbt"
```

Same SQL, same tests — different engine. This is the power of dbt's adapter abstraction.

## Query via Spark SQL

Connect to the Spark Thrift Server and query the same mart:

```bash
docker compose exec spark /opt/spark/bin/beeline -u "jdbc:hive2://localhost:10000/" -e "SELECT first_name, last_name, total_orders, total_revenue, customer_tier FROM db.customer_orders ORDER BY total_revenue DESC;"
```

Compare this output with the DuckDB results from the previous step — they should be identical.

## Explore Nessie branching

This is where Nessie gets interesting — Git-like branching for data:

```bash
docker compose exec spark /opt/spark/bin/spark-sql --conf spark.sql.catalog.nessie.ref=main << 'EOF'
-- List current tables on main branch
SHOW TABLES IN nessie.db;

-- Create a development branch
CREATE BRANCH dev IN nessie;

-- Switch to dev branch
USE REFERENCE dev IN nessie;

-- List tables (inherited from main)
SHOW TABLES IN nessie.db;
EOF
```

In a real environment, you'd make changes on `dev` and merge back to `main` — just like Git, but for your data catalog.
