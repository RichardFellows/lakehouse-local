<#
.SYNOPSIS
    Lakehouse Local - Management Script
.DESCRIPTION
    Manages the local Airflow + dbt + Spark/Iceberg/Nessie stack.
    Supports two engine modes: duckdb (default, lightweight) and spark (full stack).
.EXAMPLE
    .\lakehouse.ps1 up                # Start with DuckDB (default)
    .\lakehouse.ps1 up -Engine spark  # Start with Spark + Nessie
    .\lakehouse.ps1 spark-sql         # Interactive Spark SQL (spark mode only)
    .\lakehouse.ps1 dbt-run
#>

param(
    [Parameter(Position = 0)]
    [ValidateSet(
        "build", "up", "down", "clean", "logs", "status",
        "spark-sql", "dbt-run", "dbt-test", "dbt-debug",
        "nessie-contents", "s3-list", "notebook", "help"
    )]
    [string]$Command = "help",

    [ValidateSet("duckdb", "spark")]
    [string]$Engine = "duckdb"
)

# Resolve which compose profile and airflow service to use
$profile = $Engine
$airflowService = "airflow-$Engine"

function Write-Banner {
    Write-Host ""
    Write-Host "  Lakehouse Local" -ForegroundColor Cyan
    Write-Host "  Airflow + dbt + $($Engine.ToUpper()) engine" -ForegroundColor DarkGray
    Write-Host ""
}

switch ($Command) {
    "build" {
        Write-Host "Building containers ($Engine mode)..." -ForegroundColor Yellow
        docker compose --profile $profile build
    }

    "up" {
        Write-Host "Building and starting stack ($Engine mode)..." -ForegroundColor Yellow
        docker compose --profile $profile build
        docker compose --profile $profile up -d

        Write-Banner
        Write-Host "  Airflow UI:   http://localhost:8080  (check standalone_admin_password.txt for creds)" -ForegroundColor Green
        Write-Host "  LocalStack:   http://localhost:4566" -ForegroundColor Green

        if ($Engine -eq "spark") {
            Write-Host "  Spark UI:     http://localhost:8081" -ForegroundColor Green
            Write-Host "  Nessie API:   http://localhost:19120" -ForegroundColor Green
            Write-Host "  Spark Thrift: localhost:10000" -ForegroundColor Green
            Write-Host ""
            Write-Host "  Wait ~60s for Spark Thrift to be healthy, then trigger the DAG." -ForegroundColor DarkYellow
        }
        else {
            Write-Host ""
            Write-Host "  DuckDB mode — no Spark containers needed. Ready in ~10s." -ForegroundColor DarkYellow
        }
        Write-Host ""
    }

    "down" {
        Write-Host "Stopping stack..." -ForegroundColor Yellow
        # Stop all profiles to catch either mode
        docker compose --profile duckdb --profile spark down
    }

    "clean" {
        Write-Host "Stopping stack and removing volumes..." -ForegroundColor Red
        docker compose --profile duckdb --profile spark down -v
    }

    "logs" {
        docker compose --profile $profile logs -f
    }

    "status" {
        docker compose --profile duckdb --profile spark ps
    }

    "spark-sql" {
        if ($Engine -ne "spark") {
            Write-Host "spark-sql requires -Engine spark" -ForegroundColor Red
            Write-Host "  .\lakehouse.ps1 spark-sql -Engine spark" -ForegroundColor DarkGray
            return
        }
        Write-Host "Opening Spark SQL shell..." -ForegroundColor Yellow
        docker compose exec spark spark-sql
    }

    "dbt-run" {
        Write-Host "Running dbt models ($Engine target)..." -ForegroundColor Yellow
        docker compose exec $airflowService bash -c "cd /opt/dbt && dbt run --profiles-dir ."
    }

    "dbt-test" {
        Write-Host "Running dbt tests ($Engine target)..." -ForegroundColor Yellow
        docker compose exec $airflowService bash -c "cd /opt/dbt && dbt test --profiles-dir ."
    }

    "dbt-debug" {
        Write-Host "Running dbt debug ($Engine target)..." -ForegroundColor Yellow
        docker compose exec $airflowService bash -c "cd /opt/dbt && dbt debug --profiles-dir ."
    }

    "nessie-contents" {
        if ($Engine -ne "spark") {
            Write-Host "Nessie is only used in spark mode" -ForegroundColor DarkYellow
        }
        Write-Host "Nessie catalog contents:" -ForegroundColor Yellow
        try {
            $response = Invoke-RestMethod -Uri "http://localhost:19120/api/v2/trees/main/entries"
            $response | ConvertTo-Json -Depth 10
        }
        catch {
            Write-Host "Nessie not reachable. Is it running? (.\lakehouse.ps1 up -Engine spark)" -ForegroundColor Red
        }
    }

    "s3-list" {
        Write-Host "LocalStack S3 buckets:" -ForegroundColor Yellow
        docker compose exec localstack awslocal s3 ls
    }

    "notebook" {
        Write-Host "Starting Marimo notebook ($Engine engine)..." -ForegroundColor Yellow
        Write-Host "  Open http://localhost:2718 in your browser" -ForegroundColor Green

        if ($Engine -eq "spark") {
            docker compose exec $airflowService bash -c "cd /opt && LAKEHOUSE_ENGINE=spark SPARK_THRIFT_HOST=spark SPARK_THRIFT_PORT=10000 marimo run notebooks/explore.py --host 0.0.0.0 --port 2718"
        }
        else {
            docker compose exec $airflowService bash -c "cd /opt && LAKEHOUSE_ENGINE=duckdb LAKEHOUSE_DB=/opt/dbt/lakehouse.duckdb marimo run notebooks/explore.py --host 0.0.0.0 --port 2718"
        }
    }

    "help" {
        Write-Banner
        Write-Host "  Usage: .\lakehouse.ps1 <command> [-Engine duckdb|spark]" -ForegroundColor White
        Write-Host ""
        Write-Host "  Engine Modes:" -ForegroundColor Yellow
        Write-Host "    duckdb (default)  Lightweight — dbt runs DuckDB in-process, no JVM"
        Write-Host "    spark             Full stack — Spark Thrift + Nessie + Iceberg"
        Write-Host ""
        Write-Host "  Commands:" -ForegroundColor Yellow
        Write-Host "    build            Build containers for selected engine"
        Write-Host "    up               Build and start the stack"
        Write-Host "    down             Stop everything (both modes)"
        Write-Host "    clean            Stop and remove volumes"
        Write-Host "    logs             Tail container logs"
        Write-Host "    status           Show container status"
        Write-Host "    spark-sql        Open Spark SQL shell (spark mode only)"
        Write-Host "    dbt-run          Run dbt models"
        Write-Host "    dbt-test         Run dbt tests"
        Write-Host "    dbt-debug        Check dbt connectivity"
        Write-Host "    nessie-contents  Show Nessie catalog entries (spark mode)"
        Write-Host "    s3-list          List LocalStack S3 buckets"
        Write-Host "    notebook         Launch Marimo explorer notebook (DuckDB mode)"
        Write-Host "    help             Show this help"
        Write-Host ""
        Write-Host "  Examples:" -ForegroundColor Yellow
        Write-Host "    .\lakehouse.ps1 up                  # DuckDB mode (fast, lightweight)"
        Write-Host "    .\lakehouse.ps1 up -Engine spark    # Full Spark stack"
        Write-Host "    .\lakehouse.ps1 dbt-run             # Run models against DuckDB"
        Write-Host "    .\lakehouse.ps1 dbt-run -Engine spark  # Run models against Spark"
        Write-Host "    .\lakehouse.ps1 notebook             # Launch Marimo data explorer"
        Write-Host ""
    }
}
