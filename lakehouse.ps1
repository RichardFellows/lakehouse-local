<#
.SYNOPSIS
    Lakehouse Local - Management Script
.DESCRIPTION
    Manages the local Airflow + dbt lakehouse stack.
    Single Airflow instance with two DAGs (DuckDB + Spark) and a
    Marimo notebook that queries both engines side by side.
.EXAMPLE
    .\lakehouse.ps1 up
    .\lakehouse.ps1 dbt-run
    .\lakehouse.ps1 notebook
#>

param(
    [Parameter(Position = 0)]
    [ValidateSet(
        "build", "up", "down", "clean", "logs", "status",
        "spark-sql", "dbt-run", "dbt-test", "dbt-debug", "dbt-seed",
        "nessie-contents", "s3-list", "notebook", "help"
    )]
    [string]$Command = "help",

    [Parameter(Position = 1)]
    [ValidateSet("all", "duckdb", "spark")]
    [string]$Target = "all"
)

function Write-Banner {
    Write-Host ""
    Write-Host "  Lakehouse Local" -ForegroundColor Cyan
    Write-Host "  Airflow + dbt — DuckDB & Spark side by side" -ForegroundColor DarkGray
    Write-Host ""
}

function Run-Dbt {
    param([string]$DbtCmd)

    if ($Target -eq "all" -or $Target -eq "duckdb") {
        Write-Host "  [DuckDB] dbt $DbtCmd" -ForegroundColor DarkGray
        docker compose exec airflow bash -c "cd /opt/dbt && DBT_TARGET=duckdb dbt $DbtCmd --profiles-dir ."
        if ($LASTEXITCODE -eq 0) { Write-Host "  [DuckDB] OK" -ForegroundColor Green }
        else { Write-Host "  [DuckDB] FAILED" -ForegroundColor Red }
    }
    if ($Target -eq "all" -or $Target -eq "spark") {
        Write-Host "  [Spark] dbt $DbtCmd" -ForegroundColor DarkGray
        docker compose exec airflow bash -c "cd /opt/dbt && DBT_TARGET=spark dbt $DbtCmd --profiles-dir ."
        if ($LASTEXITCODE -eq 0) { Write-Host "  [Spark] OK" -ForegroundColor Green }
        else { Write-Host "  [Spark] FAILED" -ForegroundColor Red }
    }
}

switch ($Command) {
    "build" {
        Write-Host "Building all containers..." -ForegroundColor Yellow
        docker compose build
    }

    "up" {
        Write-Host "Building and starting stack..." -ForegroundColor Yellow
        docker compose build
        docker compose up -d

        Write-Banner
        Write-Host "  Airflow UI:  http://localhost:8080" -ForegroundColor Green
        Write-Host "  Spark UI:    http://localhost:8081" -ForegroundColor Green
        Write-Host "  Notebook:    http://localhost:2718" -ForegroundColor Green
        Write-Host "  Nessie API:  http://localhost:19120" -ForegroundColor Green
        Write-Host "  LocalStack:  http://localhost:4566" -ForegroundColor Green
        Write-Host ""
        Write-Host "  Two DAGs in Airflow: lakehouse_duckdb + lakehouse_spark" -ForegroundColor DarkYellow
        Write-Host "  Wait ~60s for Spark, then trigger both DAGs to compare." -ForegroundColor DarkYellow
        Write-Host ""
    }

    "down" {
        Write-Host "Stopping stack..." -ForegroundColor Yellow
        docker compose down
    }

    "clean" {
        Write-Host "Stopping stack and removing volumes..." -ForegroundColor Red
        docker compose down -v
    }

    "logs" {
        switch ($Target) {
            "duckdb" { docker compose logs -f airflow }
            "spark"  { docker compose logs -f airflow spark nessie }
            default  { docker compose logs -f }
        }
    }

    "status" {
        docker compose ps
    }

    "spark-sql" {
        Write-Host "Opening Spark SQL shell..." -ForegroundColor Yellow
        docker compose exec spark spark-sql
    }

    "dbt-seed" {
        Write-Host "Seeding dbt data..." -ForegroundColor Yellow
        Run-Dbt "seed"
    }

    "dbt-run" {
        Write-Host "Running dbt models..." -ForegroundColor Yellow
        Run-Dbt "run"
    }

    "dbt-test" {
        Write-Host "Running dbt tests..." -ForegroundColor Yellow
        Run-Dbt "test"
    }

    "dbt-debug" {
        Write-Host "Running dbt debug..." -ForegroundColor Yellow
        Run-Dbt "debug"
    }

    "nessie-contents" {
        Write-Host "Nessie catalog contents:" -ForegroundColor Yellow
        try {
            $response = Invoke-RestMethod -Uri "http://localhost:19120/api/v2/trees/main/entries"
            $response | ConvertTo-Json -Depth 10
        }
        catch {
            Write-Host "Nessie not reachable. Is the stack running?" -ForegroundColor Red
        }
    }

    "s3-list" {
        Write-Host "LocalStack S3 buckets:" -ForegroundColor Yellow
        docker compose exec localstack awslocal s3 ls
    }

    "notebook" {
        Write-Host "Notebook running at http://localhost:2718" -ForegroundColor Green
        Write-Host "  Queries both DuckDB and Spark side by side" -ForegroundColor DarkGray
        docker compose logs -f notebook
    }

    "help" {
        Write-Banner
        Write-Host "  Usage: .\lakehouse.ps1 <command> [target]" -ForegroundColor White
        Write-Host ""
        Write-Host "  Targets (for dbt commands, default: all):" -ForegroundColor Yellow
        Write-Host "    all       Run on both DuckDB and Spark"
        Write-Host "    duckdb    DuckDB engine only"
        Write-Host "    spark     Spark engine only"
        Write-Host ""
        Write-Host "  Commands:" -ForegroundColor Yellow
        Write-Host "    build              Build all containers"
        Write-Host "    up                 Start the full stack"
        Write-Host "    down               Stop everything"
        Write-Host "    clean              Stop and remove volumes"
        Write-Host "    logs [target]      Tail logs (all, duckdb, or spark)"
        Write-Host "    status             Show container status"
        Write-Host "    spark-sql          Open Spark SQL shell"
        Write-Host "    dbt-seed [target]  Seed reference data"
        Write-Host "    dbt-run [target]   Run dbt models"
        Write-Host "    dbt-test [target]  Run dbt tests"
        Write-Host "    dbt-debug [target] Check dbt connectivity"
        Write-Host "    nessie-contents    Show Nessie catalog entries"
        Write-Host "    s3-list            List LocalStack S3 buckets"
        Write-Host "    notebook           Show notebook logs (auto-starts with stack)"
        Write-Host "    help               Show this help"
        Write-Host ""
        Write-Host "  Quick start:" -ForegroundColor Yellow
        Write-Host "    .\lakehouse.ps1 up"
        Write-Host "    .\lakehouse.ps1 dbt-run        # Runs on BOTH engines"
        Write-Host "    # Open http://localhost:2718    # Side-by-side notebook"
        Write-Host "    # Open http://localhost:8080    # Airflow (2 DAGs)"
        Write-Host ""
        Write-Host "  Ports:" -ForegroundColor Yellow
        Write-Host "    8080   Airflow (both DAGs)"
        Write-Host "    8081   Spark UI"
        Write-Host "    2718   Marimo notebook"
        Write-Host "    19120  Nessie API"
        Write-Host "    4566   LocalStack"
        Write-Host "    10000  Spark Thrift"
        Write-Host ""
    }
}
