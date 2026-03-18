.PHONY: build up down logs spark-sql clean status

# Build all containers
build:
	docker compose build

# Start the full stack
up: build
	docker compose up -d
	@echo ""
	@echo "🚀 Stack starting..."
	@echo "  Airflow UI:   http://localhost:8080  (admin/admin — check standalone_admin_password.txt)"
	@echo "  Spark UI:     http://localhost:8081"
	@echo "  Nessie API:   http://localhost:19120"
	@echo "  LocalStack:   http://localhost:4566"
	@echo "  Spark Thrift: localhost:10000"
	@echo ""
	@echo "Wait ~60s for Spark Thrift to be healthy, then trigger the DAG from Airflow UI."

# Stop everything
down:
	docker compose down

# Stop and remove volumes
clean:
	docker compose down -v

# Tail logs
logs:
	docker compose logs -f

# Container status
status:
	docker compose ps

# Interactive Spark SQL session
spark-sql:
	docker compose exec spark spark-sql

# Run dbt manually inside Airflow container
dbt-run:
	docker compose exec airflow bash -c "cd /opt/dbt && dbt run --profiles-dir ."

dbt-test:
	docker compose exec airflow bash -c "cd /opt/dbt && dbt test --profiles-dir ."

dbt-debug:
	docker compose exec airflow bash -c "cd /opt/dbt && dbt debug --profiles-dir ."

# Check Nessie catalog contents
nessie-contents:
	@curl -s http://localhost:19120/api/v2/trees/main/entries | python3 -m json.tool

# Check LocalStack S3 buckets
s3-list:
	@docker compose exec localstack awslocal s3 ls
