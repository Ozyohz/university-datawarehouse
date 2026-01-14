# 🏛️ University Data Warehouse

A modern, open-source Data Warehouse solution for universities built with the **Medallion Architecture** (Bronze → Silver → Gold).

## 📊 Overview

This project provides a complete data warehouse infrastructure for managing and analyzing university data including:

- **Students** - Demographics, enrollment, academic progress
- **Courses** - Course catalog, scheduling, capacity
- **Enrollments** - Student-course registrations, grades
- **Finances** - Tuition, scholarships, payments
- **Academic Performance** - GPA, graduation rates, retention

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                             │
│    Databases │ CSV/Excel Files │ APIs │ External Systems         │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                    INGESTION LAYER                               │
│              Apache Airflow (Orchestration)                      │
└───────────────────────────┬─────────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────────┐
│                    STORAGE LAYER (Data Lakehouse)                │
│  ┌─────────────┐   ┌─────────────┐   ┌─────────────┐            │
│  │   BRONZE    │ → │   SILVER    │ → │    GOLD     │            │
│  │  Raw Data   │   │  Cleaned    │   │  Business   │            │
│  │  As-is      │   │  Validated  │   │  Ready      │            │
│  └─────────────┘   └─────────────┘   └─────────────┘            │
│                   MinIO + Delta Lake                             │
└───────────────────────────┬─────────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────────┐
│                    SERVING LAYER                                 │
│     Apache Superset │ MLflow │ Data APIs │ Metabase             │
└─────────────────────────────────────────────────────────────────┘
```

## 🛠️ Tech Stack

| Component | Technology |
|-----------|------------|
| **Orchestration** | Apache Airflow |
| **Processing** | Apache Spark |
| **Transformation** | dbt Core |
| **Storage** | MinIO + Delta Lake |
| **Database** | PostgreSQL |
| **BI/Dashboards** | Apache Superset |
| **ML Platform** | MLflow |
| **Data Quality** | Great Expectations |
| **Monitoring** | Prometheus + Grafana |

## 📁 Project Structure

```
university-datawarehouse/
├── docker-compose.yml          # Main Docker Compose file
├── Makefile                    # Automation commands
├── .env.example                # Environment variables template
│
├── infrastructure/             # Infrastructure & configuration
│   ├── docker/                 # Docker configurations
│   ├── scripts/                # Setup scripts
│   └── config/                 # Configuration files
│
├── dags/                       # Airflow DAGs
│   ├── ingestion/              # Data ingestion DAGs
│   ├── transformation/         # ETL transformation DAGs
│   └── quality/                # Data quality check DAGs
│
├── spark_jobs/                 # Spark ETL jobs
│   ├── bronze/                 # Raw data loaders
│   ├── silver/                 # Data cleansing
│   └── gold/                   # Business transformations
│
├── dbt/                        # dbt project
│   ├── models/                 # SQL models
│   ├── tests/                  # Data tests
│   └── macros/                 # Reusable macros
│
├── data_quality/               # Data quality framework
│   ├── great_expectations/     # GE configurations
│   └── soda/                   # Soda checks
│
├── ml/                         # Machine Learning
│   ├── notebooks/              # Jupyter notebooks
│   ├── models/                 # Model code
│   └── features/               # Feature engineering
│
├── docs/                       # Documentation
├── tests/                      # Unit & integration tests
└── monitoring/                 # Monitoring configs
```

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose
- Make (optional, for automation)
- 16GB+ RAM recommended

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd university-datawarehouse
   ```

2. **Setup environment**
   ```bash
   cp .env.example .env
   # Edit .env with your configurations
   ```

3. **Start all services**
   ```bash
   make up
   # Or: docker-compose up -d
   ```

4. **Access the services**
   - Airflow: http://localhost:8080 (admin/admin)
   - Superset: http://localhost:8088 (admin/admin)
   - MinIO: http://localhost:9001 (minioadmin/minioadmin)
   - Spark UI: http://localhost:8081
   - Grafana: http://localhost:3000 (admin/admin)

### Running ETL Pipelines

```bash
# Trigger full ETL pipeline
make run-etl

# Run specific DAG
make run-dag DAG_ID=etl_students

# Run dbt transformations
make dbt-run
```

## 📊 Data Model

### Dimensions
- `dim_student` - Student master data (SCD Type 2)
- `dim_course` - Course catalog
- `dim_instructor` - Faculty information
- `dim_semester` - Academic periods
- `dim_department` - Organizational structure
- `dim_date` - Date dimension

### Facts
- `fact_enrollment` - Student-course enrollments with grades
- `fact_tuition` - Financial transactions
- `fact_class_session` - Class attendance
- `fact_graduation` - Graduation records

## 🔐 Security

- Role-Based Access Control (RBAC)
- Column-level security for PII
- Data encryption at rest
- Audit logging enabled

## 📈 Dashboards

Pre-built dashboards available in Superset:
1. Executive Overview
2. Academic Performance
3. Student Analytics
4. Financial Dashboard
5. Operational Metrics

## 🧪 Testing

```bash
# Run all tests
make test

# Run data quality checks
make quality-check

# Run dbt tests
make dbt-test
```

## 📚 Documentation

- [Architecture Guide](docs/architecture.md)
- [Data Dictionary](docs/data_dictionary.md)
- [ETL Runbook](docs/runbooks/etl.md)
- [Troubleshooting](docs/troubleshooting.md)

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## 📄 License

This project is licensed under the MIT License.
