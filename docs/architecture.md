# University Data Warehouse - Architecture Guide

## 1. Overview

This document describes the technical architecture of the University Data Warehouse system, built using the **Medallion Architecture** pattern with open-source technologies.

---

## 2. Architecture Pattern: Medallion (Lakehouse)

The Medallion Architecture organizes data into three distinct layers, each serving a specific purpose:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              DATA LAKEHOUSE                                   │
│                                                                               │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐          │
│  │     BRONZE      │    │     SILVER      │    │      GOLD       │          │
│  │   (Raw Data)    │ →  │ (Cleaned Data)  │ →  │ (Business Data) │          │
│  │                 │    │                 │    │                 │          │
│  │ • As-is copy    │    │ • Deduplicated  │    │ • Star Schema   │          │
│  │ • All history   │    │ • Validated     │    │ • Aggregations  │          │
│  │ • Append-only   │    │ • Standardized  │    │ • Optimized     │          │
│  └─────────────────┘    └─────────────────┘    └─────────────────┘          │
│                                                                               │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.1 Bronze Layer (Raw)
- **Purpose**: Store raw data exactly as received from source systems
- **Characteristics**:
  - No transformations applied
  - Append-only (immutable)
  - Full history retained
  - Includes metadata columns (_ingested_at, _source_file, _batch_id)
- **Storage Format**: Parquet/Delta Lake

### 2.2 Silver Layer (Cleaned)
- **Purpose**: Cleanse, validate, and conform data
- **Characteristics**:
  - Deduplicated records
  - Standardized formats (dates, phone numbers, etc.)
  - Business rules validated
  - Schema enforced
  - Data quality flags added
- **Storage Format**: Delta Lake / PostgreSQL

### 2.3 Gold Layer (Business)
- **Purpose**: Business-ready data optimized for analytics
- **Characteristics**:
  - Star schema design (dimensions + facts)
  - Pre-computed aggregations
  - SCD Type 2 for historical tracking
  - Optimized for query performance
- **Storage Format**: PostgreSQL (for BI tools), Delta Lake (for ML)

---

## 3. Technology Stack

### 3.1 Core Components

| Component | Technology | Version | Purpose |
|-----------|------------|---------|---------|
| **Orchestration** | Apache Airflow | 2.8+ | Workflow scheduling & monitoring |
| **Processing** | Apache Spark | 3.5+ | Distributed data processing |
| **Transformation** | dbt Core | 1.7+ | SQL-based transformations |
| **Storage (Object)** | MinIO | Latest | S3-compatible object storage |
| **Storage (Relational)** | PostgreSQL | 15+ | Analytical database |
| **Table Format** | Delta Lake | 3.0+ | ACID transactions on data lake |

### 3.2 Supporting Tools

| Component | Technology | Purpose |
|-----------|------------|---------|
| **BI/Dashboards** | Apache Superset | Data visualization |
| **Alternative BI** | Metabase | Self-service analytics |
| **ML Platform** | MLflow | Model training & registry |
| **Data Quality** | Great Expectations | Data validation |
| **Data Quality** | Soda Core | Data monitoring |
| **Notebooks** | JupyterLab | Data exploration |
| **Caching** | Redis | Metadata & session cache |

### 3.3 Monitoring Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| **Metrics** | Prometheus | Time-series metrics |
| **Visualization** | Grafana | Dashboards & alerting |
| **Logging** | (Optional) ELK Stack | Centralized logging |

---

## 4. Data Flow

### 4.1 Batch ETL Pipeline

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   SOURCE     │     │   STAGING    │     │    TARGET    │
│   SYSTEMS    │     │    AREA      │     │  DATA MART   │
└──────┬───────┘     └──────┬───────┘     └──────┬───────┘
       │                    │                    │
       ▼                    ▼                    ▼
┌──────────────────────────────────────────────────────────┐
│                     AIRFLOW DAG                          │
│                                                          │
│  Extract    Load      Quality    Transform   Quality     │
│  (Source) → (Bronze) → Check  →  (Silver) → Check  →    │
│                                                          │
│            Build       Build     Build                   │
│         → (Dims)  →  (Facts) → (Aggs)  → Notify         │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

### 4.2 Pipeline Stages

1. **Extract**: Pull data from source systems (databases, files, APIs)
2. **Load to Bronze**: Store raw data with metadata
3. **Bronze Quality Check**: Validate data presence and basic structure
4. **Transform to Silver**: Clean, deduplicate, standardize
5. **Silver Quality Check**: Business rule validation
6. **Build Dimensions**: Update/create dimension tables (SCD Type 2)
7. **Build Facts**: Create fact tables with dimension key lookups
8. **Build Aggregations**: Pre-compute metrics for dashboards
9. **Gold Quality Check**: Final validation
10. **Notify**: Send success/failure notifications

---

## 5. Data Model

### 5.1 Star Schema Design

```
                    ┌─────────────────┐
                    │   dim_date      │
                    └────────┬────────┘
                             │
┌─────────────────┐          │          ┌─────────────────┐
│  dim_student    │          │          │   dim_course    │
└────────┬────────┘          │          └────────┬────────┘
         │                   │                   │
         │    ┌──────────────┴──────────────┐    │
         └───►│      fact_enrollment        │◄───┘
              │                             │
              │  • enrollment_key           │
              │  • student_key (FK)         │
              │  • course_key (FK)          │
              │  • semester_key (FK)        │
              │  • enrollment_date_key (FK) │
              │  • midterm_score            │
              │  • final_score              │
              │  • gpa_points               │
              │  • is_passed                │
              └──────────────┬──────────────┘
                             │
                             │
              ┌──────────────┴──────────────┐
              │                             │
     ┌────────▼────────┐         ┌──────────▼──────────┐
     │  dim_semester   │         │  dim_department     │
     └─────────────────┘         └─────────────────────┘
```

### 5.2 Slowly Changing Dimension (SCD Type 2)

For tracking historical changes in student attributes:

```sql
-- Current record
SELECT * FROM gold.dim_student WHERE student_id = 'SV001' AND is_current = TRUE;

-- Historical records
SELECT * FROM gold.dim_student WHERE student_id = 'SV001' ORDER BY effective_start_date;
```

---

## 6. Security Architecture

### 6.1 Access Control

```
┌─────────────────────────────────────────────────────────────┐
│                    ACCESS CONTROL MATRIX                     │
├─────────────────┬─────────┬─────────┬─────────┬─────────────┤
│ Role            │ Bronze  │ Silver  │ Gold    │ Admin       │
├─────────────────┼─────────┼─────────┼─────────┼─────────────┤
│ Data Engineer   │ R/W     │ R/W     │ R/W     │ No          │
│ Data Analyst    │ No      │ Read    │ Read    │ No          │
│ Data Scientist  │ No      │ Read    │ Read    │ No          │
│ Business User   │ No      │ No      │ Read*   │ No          │
│ Administrator   │ Full    │ Full    │ Full    │ Full        │
└─────────────────┴─────────┴─────────┴─────────┴─────────────┘
                                              * Limited columns
```

### 6.2 Data Protection

- **Encryption at Rest**: PostgreSQL TDE, MinIO encryption
- **Encryption in Transit**: TLS for all connections
- **PII Masking**: Column-level security for sensitive data
- **Audit Logging**: All access logged to audit schema

---

## 7. Scalability Considerations

### 7.1 Current Capacity (100GB)

| Component | Current Config | Max Capacity |
|-----------|---------------|--------------|
| PostgreSQL | Single node | ~500GB |
| Spark | 2 workers | ~1TB processing |
| MinIO | Single node | ~10TB storage |

### 7.2 Scaling Options

**Vertical Scaling** (Quick wins):
- Add memory/CPU to existing servers
- Increase PostgreSQL work_mem
- Add Spark executor memory

**Horizontal Scaling** (For growth):
- Add Spark workers
- PostgreSQL read replicas / Citus
- MinIO distributed mode
- Airflow CeleryExecutor with workers

---

## 8. Deployment Architecture

### 8.1 Docker Compose (Development/Small Production)

```
┌─────────────────────────────────────────────────────────────┐
│                     Docker Host                              │
│                                                              │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐          │
│  │  Airflow    │  │   Spark     │  │  PostgreSQL │          │
│  │  (Web+Sched)│  │ (Master+2W) │  │             │          │
│  └─────────────┘  └─────────────┘  └─────────────┘          │
│                                                              │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐          │
│  │   MinIO     │  │  Superset   │  │   Grafana   │          │
│  │             │  │             │  │             │          │
│  └─────────────┘  └─────────────┘  └─────────────┘          │
│                                                              │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐          │
│  │   Redis     │  │   MLflow    │  │  Prometheus │          │
│  │             │  │             │  │             │          │
│  └─────────────┘  └─────────────┘  └─────────────┘          │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### 8.2 Network Ports

| Service | Port | Purpose |
|---------|------|---------|
| Airflow | 8080 | Web UI |
| Superset | 8088 | BI Dashboard |
| MinIO Console | 9001 | Storage UI |
| MinIO API | 9000 | S3 API |
| Spark Master | 8081 | Spark UI |
| Grafana | 3000 | Monitoring |
| Prometheus | 9090 | Metrics |
| MLflow | 5000 | ML Platform |
| PostgreSQL | 5432 | Database |
| Jupyter | 8888 | Notebooks |
| Metabase | 3001 | Alt BI |

---

## 9. Disaster Recovery

### 9.1 Backup Strategy

| Component | Frequency | Retention | Method |
|-----------|-----------|-----------|--------|
| PostgreSQL | Daily | 30 days | pg_dump |
| MinIO | Daily | 30 days | mc mirror |
| Airflow DAGs | Git | Unlimited | Version control |
| Configurations | Git | Unlimited | Version control |

### 9.2 Recovery Procedures

See [Runbook: Disaster Recovery](runbooks/disaster_recovery.md)

---

## 10. Monitoring & Alerting

### 10.1 Key Metrics

**ETL Health**:
- DAG success/failure rate
- Task duration trends
- Queue backlog

**Database Health**:
- Connection count
- Query performance
- Storage growth

**Data Quality**:
- Validation pass rate
- Data freshness
- Record counts

### 10.2 Alert Thresholds

| Alert | Threshold | Severity |
|-------|-----------|----------|
| DAG Failed | Any failure | Critical |
| DAG Running > 2h | Duration | Warning |
| DB Connections > 80% | Percentage | Warning |
| Disk Usage > 85% | Percentage | Warning |
| Data > 24h old | Freshness | Warning |

---

## 11. Future Improvements

1. **Real-time Streaming**: Add Kafka for near-real-time data
2. **Data Catalog**: Implement DataHub for metadata management
3. **Feature Store**: Add Feast for ML feature management
4. **Data Versioning**: Implement DVC or LakeFS
5. **Cost Optimization**: Implement data lifecycle policies
