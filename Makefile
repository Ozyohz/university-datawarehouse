# University Data Warehouse - Control Center

.PHONY: help up down ps spark-shell dbt-run test clean

help: ## Hiển thị các lệnh hỗ trợ
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

up: ## Khởi động toàn bộ hạ tầng (Docker)
	docker-compose up -d

down: ## Dừng toàn bộ hạ tầng
	docker-compose down

ps: ## Kiểm tra trạng thái các container
	docker-compose ps

# Spark Utilities
spark-students: ## Chạy job biến đổi sinh viên (Silver Layer)
	docker exec -it spark-master spark-submit --master spark://spark-master:7077 /app/spark_jobs/silver/transform_students.py

# dbt Utilities
dbt-build: ## Chạy dbt build (Gold Layer & Tests)
	docker exec -it dbt-core dbt build

dbt-docs: ## Sinh tài liệu dbt
	docker exec -it dbt-core dbt docs generate && docker exec -it dbt-core dbt docs serve

# Testing
test-python: ## Chạy Unit Test cho logic Python/Spark
	pytest tests/

clean: ## Dọn dẹp các file rác, cache
	find . -type d -name "__pycache__" -exec rm -rf {} +
	rm -rf .pytest_cache
