# =============================================================================
# University Data Warehouse - Makefile
# =============================================================================

.PHONY: help up down restart logs build clean test lint \
        run-etl run-dag dbt-run dbt-test quality-check \
        init-airflow init-superset init-all

# Default target
.DEFAULT_GOAL := help

# Variables
DOCKER_COMPOSE = docker-compose
PROJECT_NAME = university-datawarehouse

# =============================================================================
# Help
# =============================================================================
help: ## Show this help message
	@echo "University Data Warehouse - Available Commands"
	@echo "=============================================="
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# =============================================================================
# Docker Operations
# =============================================================================
up: ## Start all services
	$(DOCKER_COMPOSE) up -d
	@echo "Services started. Access:"
	@echo "  - Airflow:   http://localhost:8080"
	@echo "  - Superset:  http://localhost:8088"
	@echo "  - MinIO:     http://localhost:9001"
	@echo "  - Spark UI:  http://localhost:8081"
	@echo "  - Grafana:   http://localhost:3000"
	@echo "  - MLflow:    http://localhost:5000"
	@echo "  - Jupyter:   http://localhost:8888"
	@echo "  - Metabase:  http://localhost:3001"

down: ## Stop all services
	$(DOCKER_COMPOSE) down

down-v: ## Stop all services and remove volumes
	$(DOCKER_COMPOSE) down -v

restart: ## Restart all services
	$(DOCKER_COMPOSE) restart

logs: ## View logs from all services
	$(DOCKER_COMPOSE) logs -f

logs-airflow: ## View Airflow logs
	$(DOCKER_COMPOSE) logs -f airflow-webserver airflow-scheduler

logs-spark: ## View Spark logs
	$(DOCKER_COMPOSE) logs -f spark-master spark-worker-1 spark-worker-2

build: ## Build all Docker images
	$(DOCKER_COMPOSE) build

build-no-cache: ## Build all Docker images without cache
	$(DOCKER_COMPOSE) build --no-cache

pull: ## Pull latest images
	$(DOCKER_COMPOSE) pull

# =============================================================================
# Initialization
# =============================================================================
init-all: ## Initialize all services (first-time setup)
	@echo "Starting initialization..."
	$(DOCKER_COMPOSE) up -d postgres redis minio
	@sleep 10
	$(DOCKER_COMPOSE) up airflow-init
	$(DOCKER_COMPOSE) up -d
	@echo "Waiting for services to be ready..."
	@sleep 30
	@make init-superset
	@echo "Initialization complete!"

init-airflow: ## Initialize Airflow database and admin user
	$(DOCKER_COMPOSE) up airflow-init

init-superset: ## Initialize Superset
	$(DOCKER_COMPOSE) exec superset superset db upgrade
	$(DOCKER_COMPOSE) exec superset superset fab create-admin \
		--username admin \
		--firstname Admin \
		--lastname User \
		--email admin@localhost \
		--password admin || true
	$(DOCKER_COMPOSE) exec superset superset init

# =============================================================================
# ETL Operations
# =============================================================================
run-etl: ## Run the full ETL pipeline
	$(DOCKER_COMPOSE) exec airflow-webserver airflow dags trigger etl_full_pipeline

run-dag: ## Run a specific DAG (usage: make run-dag DAG_ID=dag_name)
	@if [ -z "$(DAG_ID)" ]; then \
		echo "Error: DAG_ID is required. Usage: make run-dag DAG_ID=dag_name"; \
		exit 1; \
	fi
	$(DOCKER_COMPOSE) exec airflow-webserver airflow dags trigger $(DAG_ID)

pause-dag: ## Pause a DAG (usage: make pause-dag DAG_ID=dag_name)
	$(DOCKER_COMPOSE) exec airflow-webserver airflow dags pause $(DAG_ID)

unpause-dag: ## Unpause a DAG (usage: make unpause-dag DAG_ID=dag_name)
	$(DOCKER_COMPOSE) exec airflow-webserver airflow dags unpause $(DAG_ID)

list-dags: ## List all DAGs
	$(DOCKER_COMPOSE) exec airflow-webserver airflow dags list

# =============================================================================
# dbt Operations
# =============================================================================
dbt-run: ## Run dbt models
	$(DOCKER_COMPOSE) exec airflow-webserver dbt run --project-dir /opt/airflow/dbt

dbt-test: ## Run dbt tests
	$(DOCKER_COMPOSE) exec airflow-webserver dbt test --project-dir /opt/airflow/dbt

dbt-docs: ## Generate dbt documentation
	$(DOCKER_COMPOSE) exec airflow-webserver dbt docs generate --project-dir /opt/airflow/dbt

dbt-debug: ## Debug dbt connection
	$(DOCKER_COMPOSE) exec airflow-webserver dbt debug --project-dir /opt/airflow/dbt

# =============================================================================
# Spark Operations
# =============================================================================
spark-submit: ## Submit a Spark job (usage: make spark-submit JOB=path/to/job.py)
	@if [ -z "$(JOB)" ]; then \
		echo "Error: JOB is required. Usage: make spark-submit JOB=path/to/job.py"; \
		exit 1; \
	fi
	$(DOCKER_COMPOSE) exec spark-master spark-submit \
		--master spark://spark-master:7077 \
		/opt/spark/jobs/$(JOB)

spark-shell: ## Open Spark shell
	$(DOCKER_COMPOSE) exec spark-master spark-shell

pyspark: ## Open PySpark shell
	$(DOCKER_COMPOSE) exec spark-master pyspark

# =============================================================================
# Data Quality
# =============================================================================
quality-check: ## Run all data quality checks
	$(DOCKER_COMPOSE) exec airflow-webserver great_expectations checkpoint run all_checkpoint

quality-bronze: ## Run Bronze layer quality checks
	$(DOCKER_COMPOSE) exec airflow-webserver great_expectations checkpoint run bronze_checkpoint

quality-silver: ## Run Silver layer quality checks
	$(DOCKER_COMPOSE) exec airflow-webserver great_expectations checkpoint run silver_checkpoint

quality-gold: ## Run Gold layer quality checks
	$(DOCKER_COMPOSE) exec airflow-webserver great_expectations checkpoint run gold_checkpoint

# =============================================================================
# Testing
# =============================================================================
test: ## Run all tests
	$(DOCKER_COMPOSE) exec airflow-webserver pytest /opt/airflow/tests -v

test-unit: ## Run unit tests only
	$(DOCKER_COMPOSE) exec airflow-webserver pytest /opt/airflow/tests/unit -v

test-integration: ## Run integration tests only
	$(DOCKER_COMPOSE) exec airflow-webserver pytest /opt/airflow/tests/integration -v

# =============================================================================
# Linting & Formatting
# =============================================================================
lint: ## Run linters
	$(DOCKER_COMPOSE) exec airflow-webserver flake8 /opt/airflow/dags /opt/airflow/spark_jobs
	$(DOCKER_COMPOSE) exec airflow-webserver sqlfluff lint /opt/airflow/dbt/models

format: ## Format code
	$(DOCKER_COMPOSE) exec airflow-webserver black /opt/airflow/dags /opt/airflow/spark_jobs
	$(DOCKER_COMPOSE) exec airflow-webserver isort /opt/airflow/dags /opt/airflow/spark_jobs

# =============================================================================
# Database Operations
# =============================================================================
psql: ## Connect to PostgreSQL
	$(DOCKER_COMPOSE) exec postgres psql -U datawarehouse -d university_dw

db-backup: ## Backup the database
	@mkdir -p backups
	$(DOCKER_COMPOSE) exec postgres pg_dump -U datawarehouse university_dw > backups/backup_$$(date +%Y%m%d_%H%M%S).sql
	@echo "Backup created in backups/"

db-restore: ## Restore from backup (usage: make db-restore FILE=backup.sql)
	@if [ -z "$(FILE)" ]; then \
		echo "Error: FILE is required. Usage: make db-restore FILE=backup.sql"; \
		exit 1; \
	fi
	cat backups/$(FILE) | $(DOCKER_COMPOSE) exec -T postgres psql -U datawarehouse university_dw

# =============================================================================
# Cleanup
# =============================================================================
clean: ## Clean up temporary files and caches
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true

clean-all: down-v clean ## Full cleanup including volumes
	docker system prune -f

# =============================================================================
# Development
# =============================================================================
shell-airflow: ## Shell into Airflow container
	$(DOCKER_COMPOSE) exec airflow-webserver bash

shell-spark: ## Shell into Spark master container
	$(DOCKER_COMPOSE) exec spark-master bash

shell-postgres: ## Shell into PostgreSQL container
	$(DOCKER_COMPOSE) exec postgres bash

# =============================================================================
# Status & Info
# =============================================================================
status: ## Show status of all services
	$(DOCKER_COMPOSE) ps

health: ## Check health of all services
	@echo "Checking service health..."
	@echo "\nPostgreSQL:"
	@$(DOCKER_COMPOSE) exec postgres pg_isready -U datawarehouse || echo "Not ready"
	@echo "\nRedis:"
	@$(DOCKER_COMPOSE) exec redis redis-cli ping || echo "Not ready"
	@echo "\nMinIO:"
	@curl -s http://localhost:9000/minio/health/live || echo "Not ready"
	@echo "\nAirflow:"
	@curl -s http://localhost:8080/health | jq . || echo "Not ready"

info: ## Show connection information
	@echo "=============================================="
	@echo "University Data Warehouse - Service URLs"
	@echo "=============================================="
	@echo "Airflow:     http://localhost:8080  (admin/admin)"
	@echo "Superset:    http://localhost:8088  (admin/admin)"
	@echo "MinIO:       http://localhost:9001  (minioadmin/minioadmin123)"
	@echo "Spark UI:    http://localhost:8081"
	@echo "Grafana:     http://localhost:3000  (admin/admin)"
	@echo "Prometheus:  http://localhost:9090"
	@echo "MLflow:      http://localhost:5000"
	@echo "Jupyter:     http://localhost:8888  (token: jupyter)"
	@echo "Metabase:    http://localhost:3001"
	@echo "PostgreSQL:  localhost:5432        (datawarehouse/datawarehouse)"
	@echo "=============================================="
