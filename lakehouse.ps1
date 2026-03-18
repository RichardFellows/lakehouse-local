<#
.SYNOPSIS
    Lakehouse Local - Management Script
.DESCRIPTION
    Manages the local Airflow + dbt + Spark/Iceberg/Nessie stack.
.EXAMPLE
    .\lakehouse.ps1 up
    .\lakehouse.ps1 spark-sql
    .\lakehouse.ps1 dbt-run
#>

param(
    [Parameter(Position = 0)]
    [ValidateSet(
        "build", "up", "down", "clean", "logs", "status",
        "spark-sql", "dbt-run", "dbt-test", "dbt-debug",
        "nessie-contents", "s3-list", "help"
    )]
    [string]$Command = "help"
)

function Write-Banner {
    Write-Host ""
    Write-Host "  Lakehouse Local" -ForegroundColor Cyan
    Write-Host "  Airflow + dbt + Spark/Iceberg/Nessie" -ForegroundColor DarkGray
    Write-Host ""
}

switch ($Command) {
    "build" {
        Write-Host "Building containers..." -ForegroundColor Yellow
        docker compose build
    }

    "up" {
        Write-Host "Building and starting stack..." -ForegroundColor Yellow
        docker compose build
        docker compose up -d

        Write-Banner
        Write-Host "  Airflow UI:   http://localhost:8080  (check standalone_admin_password.txt for creds)" -ForegroundColor Green
        Write-Host "  Spark UI:     http://localhost:8081" -ForegroundColor Green
        Write-Host "  Nessie API:   http://localhost:19120" -ForegroundColor Green
        Write-Host "  LocalStack:   http://localhost:4566" -ForegroundColor Green
        Write-Host "  Spark Thrift: localhost:10000" -ForegroundColor Green
        Write-Host ""
        Write-Host "  Wait ~60s for Spark Thrift to be healthy, then trigger the DAG from Airflow UI." -ForegroundColor DarkYellow
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
        docker compose logs -f
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
        docker compose exec airflow bash -c "cd /opt/dbt && dbt run --profiles-dir ."
    }

    "dbt-test" {
        Write-Host "Running dbt tests..." -ForegroundColor Yellow
        docker compose exec airflow bash -c "cd /opt/dbt && dbt test --profiles-dir ."
    }

    "dbt-debug" {
        Write-Host "Running dbt debug..." -ForegroundColor Yellow
        docker compose exec airflow bash -c "cd /opt/dbt && dbt debug --profiles-dir ."
    }

    "nessie-contents" {
        Write-Host "Nessie catalog contents:" -ForegroundColor Yellow
        $response = Invoke-RestMethod -Uri "http://localhost:19120/api/v2/trees/main/entries"
        $response | ConvertTo-Json -Depth 10
    }

    "s3-list" {
        Write-Host "LocalStack S3 buckets:" -ForegroundColor Yellow
        docker compose exec localstack awslocal s3 ls
    }

    "help" {
        Write-Banner
        Write-Host "  Usage: .\lakehouse.ps1 <command>" -ForegroundColor White
        Write-Host ""
        Write-Host "  Commands:" -ForegroundColor Yellow
        Write-Host "    build            Build all containers"
        Write-Host "    up               Build and start the full stack"
        Write-Host "    down             Stop everything"
        Write-Host "    clean            Stop and remove volumes"
        Write-Host "    logs             Tail container logs"
        Write-Host "    status           Show container status"
        Write-Host "    spark-sql        Open interactive Spark SQL shell"
        Write-Host "    dbt-run          Run dbt models"
        Write-Host "    dbt-test         Run dbt tests"
        Write-Host "    dbt-debug        Run dbt debug (check connectivity)"
        Write-Host "    nessie-contents  Show Nessie catalog entries"
        Write-Host "    s3-list          List LocalStack S3 buckets"
        Write-Host "    help             Show this help"
        Write-Host ""
    }
}
