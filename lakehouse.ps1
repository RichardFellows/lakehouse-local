<#
.SYNOPSIS
    Lakehouse Local - Management Script
.DESCRIPTION
    Manages the local Airflow + dbt lakehouse stack.
    Runs both DuckDB and Spark engines side by side to demonstrate
    the same dbt transforms running on different targets.
.EXAMPLE
    .\lakehouse.ps1 up              # Start both engines
    .\lakehouse.ps1 dbt-run         # Run dbt on both engines
    .\lakehouse.ps1 dbt-run duckdb  # Run dbt on DuckDB only
    .\lakehouse.ps1 notebook duckdb # Launch DuckDB notebook
#>

param(
    [Parameter(Position = 0)]
    [ValidateSet(
        "build", "up", "down", "clean", "logs", "status",
        "spark-sql", "dbt-run", "dbt-test", "dbt-debug",
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

function Run-OnTarget {
    param(
        [string]$Label,
        [string]$Service,
        [string]$Cmd
    )
    Write-Host "  [$Label] $Cmd" -ForegroundColor DarkGray
    docker compose exec $Service bash -c $Cmd
    if ($LASTEXITCODE -ne 0) {
        Write-Host "  [$Label] FAILED (exit code $LASTEXITCODE)" -ForegroundColor Red
    }
    else {
        Write-Host "  [$Label] OK" -ForegroundColor Green
    }
}

function Run-Dbt {
    param([string]$DbtCmd)
    $fullCmd = "cd /opt/dbt && dbt $DbtCmd --profiles-dir ."

    if ($Target -eq "all" -or $Target -eq "duckdb") {
        Run-OnTarget -Label "DuckDB" -Service "airflow-duckdb" -Cmd $fullCmd
    }
    if ($Target -eq "all" -or $Target -eq "spark") {
        Run-OnTarget -Label "Spark" -Service "airflow-spark" -Cmd $fullCmd
    }
}

switch ($Command) {
    "build" {
        Write-Host "Building all containers..." -ForegroundColor Yellow
        docker compose build
    }

    "up" {
        Write-Host "Building and starting full stack..." -ForegroundColor Yellow
        docker compose build
        docker compose up -d

        Write-Banner
        Write-Host "  DuckDB Airflow:  http://localhost:8080" -ForegroundColor Green
        Write-Host "  Spark Airflow:   http://localhost:8090" -ForegroundColor Green
        Write-Host "  Spark UI:        http://localhost:8081" -ForegroundColor Green
        Write-Host "  Nessie API:      http://localhost:19120" -ForegroundColor Green
        Write-Host "  LocalStack:      http://localhost:4566" -ForegroundColor Green
        Write-Host ""
        Write-Host "  DuckDB notebook: http://localhost:2718  (.\lakehouse.ps1 notebook duckdb)" -ForegroundColor DarkGray
        Write-Host "  Spark notebook:  http://localhost:2719  (.\lakehouse.ps1 notebook spark)" -ForegroundColor DarkGray
        Write-Host ""
        Write-Host "  Wait ~60s for Spark Thrift to be healthy." -ForegroundColor DarkYellow
        Write-Host "  Both Airflow UIs have the same DAG — trigger each to see dbt run on both engines." -ForegroundColor DarkYellow
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
        if ($Target -eq "duckdb") {
            docker compose logs -f airflow-duckdb
        }
        elseif ($Target -eq "spark") {
            docker compose logs -f airflow-spark spark nessie
        }
        else {
            docker compose logs -f
        }
    }

    "status" {
        docker compose ps
    }

    "spark-sql" {
        Write-Host "Opening Spark SQL shell..." -ForegroundColor Yellow
        docker compose exec spark spark-sql
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
        Write-Host "Starting side-by-side notebook on http://localhost:2719 ..." -ForegroundColor Yellow
        Write-Host "  Connects to both DuckDB and Spark simultaneously" -ForegroundColor DarkGray
        docker compose exec airflow-spark bash -c "cd /opt && LAKEHOUSE_DB=/opt/dbt/lakehouse.duckdb SPARK_THRIFT_HOST=spark SPARK_THRIFT_PORT=10000 marimo run notebooks/explore.py --host 0.0.0.0 --port 2718"
    }

    "help" {
        Write-Banner
        Write-Host "  Usage: .\lakehouse.ps1 <command> [target]" -ForegroundColor White
        Write-Host ""
        Write-Host "  Targets (optional, default: all):" -ForegroundColor Yellow
        Write-Host "    all       Both DuckDB and Spark"
        Write-Host "    duckdb    DuckDB engine only"
        Write-Host "    spark     Spark engine only"
        Write-Host ""
        Write-Host "  Commands:" -ForegroundColor Yellow
        Write-Host "    build            Build all containers"
        Write-Host "    up               Start the full stack (both engines)"
        Write-Host "    down             Stop everything"
        Write-Host "    clean            Stop and remove volumes"
        Write-Host "    logs [target]    Tail logs (all, duckdb, or spark)"
        Write-Host "    status           Show container status"
        Write-Host "    spark-sql        Open Spark SQL shell"
        Write-Host "    dbt-run [target] Run dbt models"
        Write-Host "    dbt-test [target] Run dbt tests"
        Write-Host "    dbt-debug [target] Check dbt connectivity"
        Write-Host "    nessie-contents  Show Nessie catalog entries"
        Write-Host "    s3-list          List LocalStack S3 buckets"
        Write-Host "    notebook         Launch Marimo notebook (queries both engines)"
        Write-Host "    help             Show this help"
        Write-Host ""
        Write-Host "  Examples:" -ForegroundColor Yellow
        Write-Host "    .\lakehouse.ps1 up                  # Start everything"
        Write-Host "    .\lakehouse.ps1 dbt-run             # Run dbt on BOTH engines"
        Write-Host "    .\lakehouse.ps1 dbt-run duckdb      # Run dbt on DuckDB only"
        Write-Host "    .\lakehouse.ps1 dbt-run spark       # Run dbt on Spark only"
        Write-Host "    .\lakehouse.ps1 notebook             # Side-by-side explorer (port 2719)"
        Write-Host ""
        Write-Host "  Ports:" -ForegroundColor Yellow
        Write-Host "    8080  Airflow (DuckDB)"
        Write-Host "    8090  Airflow (Spark)"
        Write-Host "    8081  Spark UI"
        Write-Host "    2718  Marimo notebook (DuckDB)"
        Write-Host "    2719  Marimo notebook (Spark)"
        Write-Host "    19120 Nessie API"
        Write-Host "    4566  LocalStack"
        Write-Host ""
    }
}
