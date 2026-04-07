## Code Review — `lakehouse-local`

| # | Severity | Issue | File / Location |
|---|----------|-------|-----------------|
| 1 | ~~🟡 Functional~~ ✅ Fixed | `spark-sql` and `nessie-contents` commands don't fully respect `-Target` — they check if services are running but ignore the parameter value | `lakehouse.ps1` → added `-Target duckdb` guard |
| 2 | ~~🟠 Reliability~~ ✅ Already resolved | dbt compiled artifacts (`dbt_project/target/`) tracked in git — already in `.gitignore` and not tracked | `.gitignore` |
| 3 | ~~🔵 Minor~~ ✅ Fixed | Spark version drift — README said "Spark 3.5.4" but Dockerfile installs 3.5.8 | `README.md` → updated to 3.5.8 |
| 4 | ~~🔵 Minor~~ ✅ Fixed | Undocumented DAG — `file_ingest.py` exists in `dags/` but wasn't mentioned in README | `README.md` → added DAGs table and Quick Start mention |
