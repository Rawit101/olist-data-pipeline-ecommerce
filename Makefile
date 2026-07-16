# ============================================================
# Makefile — Olist Data Pipeline Quick Commands
# ============================================================

.PHONY: up down restart logs ps build init clean

# Start all services
up:
	docker compose up -d

# Stop all services
down:
	docker compose down

# Rebuild and start
build:
	docker compose build --no-cache
	docker compose up -d

# Restart all services
restart:
	docker compose down
	docker compose up -d

# View logs (all services)
logs:
	docker compose logs -f

# View logs for specific service
logs-airflow:
	docker compose logs -f airflow-webserver airflow-scheduler airflow-worker

logs-minio:
	docker compose logs -f minio

# Show running containers
ps:
	docker compose ps

# Initialize (first time setup)
init:
	@echo "===== Creating directories ====="
	mkdir -p data/raw data/processed data/warehouse data/rejected
	mkdir -p exports logs plugins reports
	mkdir -p dags config src
	@echo "===== Copying env files ====="
	cp -n .env.example .env 2>/dev/null || true
	cp -n config/airflow.env.example config/airflow.env 2>/dev/null || true
	cp -n config/minio.env.example config/minio.env 2>/dev/null || true
	@echo "===== Building Docker images ====="
	docker compose build
	@echo "===== Starting services ====="
	docker compose up -d
	@echo ""
	@echo "===== Setup Complete ====="
	@echo "Airflow UI:    http://localhost:8080  (admin/admin)"
	@echo "MinIO Console: http://localhost:9001  (minioadmin/minioadmin123)"

# Clean everything (WARNING: removes data)
clean:
	docker compose down -v --remove-orphans
	rm -rf logs/*.log

# Run dbt
dbt-run:
	docker compose exec airflow-worker bash -c "cd /opt/airflow/dbt_olist && dbt run"

dbt-test:
	docker compose exec airflow-worker bash -c "cd /opt/airflow/dbt_olist && dbt test"

dbt-docs:
	docker compose exec airflow-worker bash -c "cd /opt/airflow/dbt_olist && dbt docs generate"

# Run pytest
test:
	docker compose exec airflow-worker bash -c "cd /opt/airflow && pytest tests/ -v"

# Check DAGs
check-dags:
	docker compose exec airflow-webserver airflow dags list
