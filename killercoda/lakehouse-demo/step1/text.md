# Clone and explore the project

First, let's clone the repository and take a look at what we're working with.

```bash
git clone https://github.com/RichardFellows/lakehouse-local.git
cd lakehouse-local
```

## Project structure

Take a look at the key files:

```bash
find . -maxdepth 2 -not -path './.git/*' -not -path './killercoda/*' -not -path './.devcontainer/*' | sort
```

The important directories are:

| Directory | Purpose |
|-----------|---------|
| `dbt_project/` | dbt models, seeds, and profiles |
| `dags/` | Airflow DAG definitions |
| `notebooks/` | Marimo notebooks for exploration |
| `airflow/` | Airflow Dockerfile (UBI8-based) |
| `spark/` | Spark Dockerfile + Maven dependencies |
| `feeds/` | Sample CSV data files |

## Examine the dbt profile

The profile dynamically selects the engine based on an environment variable:

```bash
cat dbt_project/profiles.yml
```

Notice: `target: "{{ env_var('DBT_TARGET', 'duckdb') }}"` — the same models run on both DuckDB and Spark with zero code changes.

## Check Docker Compose

```bash
cat docker-compose.yml
```

The compose file defines all services. In DuckDB mode only Airflow + LocalStack are needed. Spark mode adds the Spark Thrift Server and Nessie catalog.
