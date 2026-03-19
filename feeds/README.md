# Feed Files Landing Zone

Simulates CSV file delivery from an upstream provider (e.g. via SFTP).

## How It Works

1. Drop CSV files into `incoming/`
2. The `file_ingest` Airflow DAG polls every 5 minutes
3. Files are uploaded to S3 (`raw-data` bucket)
4. Data is loaded into Iceberg tables under the `raw` namespace
5. Processed files are moved to `archive/` with a timestamp
6. dbt pipeline runs automatically to rebuild marts

## File Naming

- `products.csv` → creates/appends to `nessie.raw.products`
- `transactions_20240615.csv` → creates/appends to `nessie.raw.transactions`

The table name is derived from the filename (without date suffix).

## Example

```bash
# Copy a sample file
cp sample_products.csv incoming/products.csv

# Wait for the DAG to run (every 5 min) or trigger manually in Airflow UI
```

## Directories

- `incoming/` — drop files here (watched by Airflow)
- `archive/` — processed files (timestamped, kept for audit)
