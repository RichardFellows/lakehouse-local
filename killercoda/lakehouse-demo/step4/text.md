# Run the dbt pipeline via Airflow

Now let's trigger the DuckDB pipeline. We'll do this via the Airflow CLI (you could also trigger it from the web UI on port 8082).

## Trigger the DAG

```bash
cd /root/lakehouse-local
docker compose exec airflow airflow dags unpause lakehouse_duckdb
docker compose exec airflow airflow dags trigger lakehouse_duckdb
```

## Watch it run

Monitor the DAG run until it completes:

```bash
echo "⏳ Waiting for DAG run to complete..."
while true; do
  STATE=$(docker compose exec airflow airflow dags list-runs -d lakehouse_duckdb -o plain 2>/dev/null | tail -1 | awk '{print $3}')
  if [ "$STATE" = "success" ]; then
    echo "✅ DAG completed successfully!"
    break
  elif [ "$STATE" = "failed" ]; then
    echo "❌ DAG failed. Check logs with: docker compose logs airflow"
    break
  fi
  echo "  Status: ${STATE:-starting}..."
  sleep 5
done
```

## Verify dbt ran correctly

Let's also run dbt directly to see the output:

```bash
docker compose exec airflow bash -c "cd /opt/dbt && DBT_TARGET=duckdb dbt run --profiles-dir /opt/dbt"
```

You should see dbt compile and run the staging views and mart table.

## Run dbt tests

```bash
docker compose exec airflow bash -c "cd /opt/dbt && DBT_TARGET=duckdb dbt test --profiles-dir /opt/dbt"
```

All tests should pass — unique constraints, not-null checks, and referential integrity.
