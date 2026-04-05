## Code Review Summary — `lakehouse-local`

| # | Severity | Issue | File / Location |
|---|----------|-------|-----------------|
| 1 | ~~🔴 Critical~~ ✅ Fixed | Docker Compose profiles described in docs but not implemented — all services always start | `docker-compose.yml`, `lakehouse.ps1`, `README.md` |
| 2 | ~~🔴 Critical~~ ✅ Fixed | Airflow always waits on Spark healthcheck (up to 5 min), even in DuckDB-only usage | `docker-compose.yml` → removed `airflow.depends_on.spark` |
| 3 | ~~🔴 Critical~~ ✅ Fixed | Airflow port mismatch — compose exposes `8082` but script and docs say `8080` | `lakehouse.ps1` → `up` and `help` blocks; `README.md` → Quick Start |
| 4 | ~~🔴 Critical~~ ✅ Fixed | `-Engine` parameter referenced throughout README doesn't exist — actual param is `-Target` | `README.md` → Quick Start, Troubleshooting, Nessie branching sections |
| 5 | 🟡 Functional | Nessie has no `depends_on: localstack` but references it for S3 — race condition on cold start | `docker-compose.yml` → `nessie` service (missing `depends_on`) |
| 6 | 🟡 Functional | Notebook starts before Spark is healthy — Spark queries will fail on startup | `docker-compose.yml` → `notebook.depends_on` (only lists `localstack`) |
| 7 | 🟡 Functional | `dbt-run` with default target `all` attempts Spark dbt with no check that Spark is up | `lakehouse.ps1` → `Run-Dbt` function; `switch` block `"dbt-run"` case |
| 8 | 🟡 Functional | `DBT_TARGET` env var not set in Airflow container — dbt always defaults to `duckdb` target | `docker-compose.yml` → `airflow.environment` (variable absent); `dbt_project/profiles.yml` → `env_var('DBT_TARGET', 'duckdb')` |
| 9 | 🟠 Reliability | `localstack` image unpinned (`latest`) — all other images are version-pinned | `docker-compose.yml` → `localstack.image: localstack/localstack:latest` |
| 10 | ~~🟠 Reliability~~ ✅ Fixed | No exit-code guard after `docker compose build` — success banner prints even on build failure | `lakehouse.ps1` → `"up"` switch case |
| 11 | 🟠 Reliability | `spark-sql` and `nessie-contents` commands ignore `-Target` and run unconditionally | `lakehouse.ps1` → `"spark-sql"` and `"nessie-contents"` switch cases |
| 12 | 🔵 Minor | Confusing naming: `notebook/` (Dockerfile) vs `notebooks/` (volume-mounted Python files) | `docker-compose.yml` → `notebook.build.context` and `notebook.volumes`; repo root directory listing |
| 13 | 🔵 Minor | `airflow standalone` used with no comment — dev-only mode, no auth, single-user | `docker-compose.yml` → `airflow.command: airflow standalone` |
| 14 | 🔵 Minor | SQLite + SequentialExecutor means DAGs queue serially — worth documenting the limitation | `docker-compose.yml` → `airflow.environment.AIRFLOW__CORE__EXECUTOR` and `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` |