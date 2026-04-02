# Start the stack (DuckDB mode)

Let's bring up the lightweight DuckDB stack first. This only needs two containers: **Airflow** and **LocalStack** (S3).

```bash
cd /root/lakehouse-local
docker compose up -d localstack airflow
```

This will build the Airflow image (UBI8-based with dbt-duckdb installed) and start LocalStack for S3-compatible storage.

## Wait for services

The Airflow image build takes a couple of minutes on first run. Let's wait for everything to be ready:

```bash
echo "⏳ Waiting for LocalStack..."
until curl -sf http://localhost:4566/_localstack/health > /dev/null 2>&1; do sleep 2; done
echo "✅ LocalStack ready"

echo "⏳ Waiting for Airflow (this may take 2-3 minutes on first build)..."
until curl -sf http://localhost:8082/health > /dev/null 2>&1; do sleep 5; done
echo "✅ Airflow ready"
```

## Check S3 buckets

LocalStack automatically creates the S3 buckets via an init script:

```bash
docker compose exec localstack awslocal s3 ls
```

You should see `warehouse` and `raw-data` buckets — these simulate AWS S3 locally.

## Access Airflow UI

The Airflow UI is running on port 8082. You can access it via the **Airflow UI** tab at the top.

Default credentials: `admin` / the password shown in the Airflow startup logs:

```bash
docker compose logs airflow 2>&1 | grep -i password
```
