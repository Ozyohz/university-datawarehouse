-- =============================================================================
-- University Data Warehouse - Database Initialization Script
-- =============================================================================

-- Create schemas for each layer
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS audit;

-- =============================================================================
-- BRONZE LAYER - Raw Tables (as-is from source)
-- =============================================================================

-- Raw Students
CREATE TABLE IF NOT EXISTS bronze.raw_students (
    id SERIAL,
    student_id VARCHAR(50),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    date_of_birth DATE,
    gender VARCHAR(10),
    email VARCHAR(200),
    phone VARCHAR(20),
    address TEXT,
    city VARCHAR(100),
    province VARCHAR(100),
    cohort_year INTEGER,
    department_id VARCHAR(50),
    enrollment_date DATE,
    status VARCHAR(50),
    _source_file VARCHAR(500),
    _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _batch_id VARCHAR(50)
);

-- Raw Courses
CREATE TABLE IF NOT EXISTS bronze.raw_courses (
    id SERIAL,
    course_id VARCHAR(50),
    course_name VARCHAR(200),
    course_description TEXT,
    credits INTEGER,
    department_id VARCHAR(50),
    course_level VARCHAR(50),
    course_type VARCHAR(50),
    is_active BOOLEAN,
    _source_file VARCHAR(500),
    _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _batch_id VARCHAR(50)
);

-- Raw Enrollments
CREATE TABLE IF NOT EXISTS bronze.raw_enrollments (
    id SERIAL,
    enrollment_id VARCHAR(50),
    student_id VARCHAR(50),
    course_id VARCHAR(50),
    semester_id VARCHAR(50),
    enrollment_date DATE,
    midterm_score DECIMAL(5,2),
    final_score DECIMAL(5,2),
    total_score DECIMAL(5,2),
    grade_letter VARCHAR(5),
    status VARCHAR(50),
    attendance_count INTEGER,
    absence_count INTEGER,
    _source_file VARCHAR(500),
    _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _batch_id VARCHAR(50)
);

-- Raw Tuition
CREATE TABLE IF NOT EXISTS bronze.raw_tuition (
    id SERIAL,
    transaction_id VARCHAR(50),
    student_id VARCHAR(50),
    semester_id VARCHAR(50),
    tuition_amount DECIMAL(15,2),
    scholarship_amount DECIMAL(15,2),
    discount_amount DECIMAL(15,2),
    paid_amount DECIMAL(15,2),
    payment_date DATE,
    payment_method VARCHAR(50),
    status VARCHAR(50),
    _source_file VARCHAR(500),
    _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _batch_id VARCHAR(50)
);

-- Raw Semesters
CREATE TABLE IF NOT EXISTS bronze.raw_semesters (
    id SERIAL,
    semester_id VARCHAR(50),
    semester_name VARCHAR(100),
    academic_year INTEGER,
    semester_number INTEGER,
    start_date DATE,
    end_date DATE,
    is_current BOOLEAN,
    _source_file VARCHAR(500),
    _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _batch_id VARCHAR(50)
);

-- Raw Departments
CREATE TABLE IF NOT EXISTS bronze.raw_departments (
    id SERIAL,
    department_id VARCHAR(50),
    department_name VARCHAR(200),
    faculty VARCHAR(200),
    dean_name VARCHAR(100),
    established_year INTEGER,
    _source_file VARCHAR(500),
    _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _batch_id VARCHAR(50)
);

-- Raw Instructors
CREATE TABLE IF NOT EXISTS bronze.raw_instructors (
    id SERIAL,
    instructor_id VARCHAR(50),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(200),
    department_id VARCHAR(50),
    academic_rank VARCHAR(100),
    degree VARCHAR(100),
    hire_date DATE,
    is_active BOOLEAN,
    _source_file VARCHAR(500),
    _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _batch_id VARCHAR(50)
);

-- =============================================================================
-- SILVER LAYER - Cleaned and Validated Tables
-- =============================================================================

-- Cleaned Students
CREATE TABLE IF NOT EXISTS silver.stg_students (
    student_key SERIAL PRIMARY KEY,
    student_id VARCHAR(50) NOT NULL UNIQUE,
    full_name VARCHAR(200),
    date_of_birth DATE,
    gender VARCHAR(10),
    email VARCHAR(200),
    phone VARCHAR(20),
    address TEXT,
    city VARCHAR(100),
    province VARCHAR(100),
    cohort_year INTEGER,
    department_id VARCHAR(50),
    enrollment_date DATE,
    graduation_date DATE,
    status VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_valid BOOLEAN DEFAULT TRUE,
    validation_errors TEXT[]
);

-- Cleaned Courses
CREATE TABLE IF NOT EXISTS silver.stg_courses (
    course_key SERIAL PRIMARY KEY,
    course_id VARCHAR(50) NOT NULL UNIQUE,
    course_name VARCHAR(200),
    course_description TEXT,
    credits INTEGER,
    department_id VARCHAR(50),
    course_level VARCHAR(50),
    course_type VARCHAR(50),
    is_active BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_valid BOOLEAN DEFAULT TRUE,
    validation_errors TEXT[]
);

-- Cleaned Enrollments
CREATE TABLE IF NOT EXISTS silver.stg_enrollments (
    enrollment_key SERIAL PRIMARY KEY,
    enrollment_id VARCHAR(50) NOT NULL UNIQUE,
    student_id VARCHAR(50),
    course_id VARCHAR(50),
    semester_id VARCHAR(50),
    enrollment_date DATE,
    midterm_score DECIMAL(5,2),
    final_score DECIMAL(5,2),
    total_score DECIMAL(5,2),
    grade_letter VARCHAR(5),
    gpa_points DECIMAL(3,2),
    status VARCHAR(50),
    attendance_count INTEGER,
    absence_count INTEGER,
    attendance_rate DECIMAL(5,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_valid BOOLEAN DEFAULT TRUE,
    validation_errors TEXT[]
);

-- =============================================================================
-- GOLD LAYER - Business-Ready Dimension and Fact Tables
-- =============================================================================

-- Dimension: Date
CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    month_name VARCHAR(20),
    week INTEGER,
    day INTEGER,
    day_name VARCHAR(20),
    day_of_week INTEGER,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    fiscal_year INTEGER,
    fiscal_quarter INTEGER
);

-- Dimension: Student (SCD Type 2)
CREATE TABLE IF NOT EXISTS gold.dim_student (
    student_key SERIAL PRIMARY KEY,
    student_id VARCHAR(50) NOT NULL,
    full_name VARCHAR(200),
    date_of_birth DATE,
    age_group VARCHAR(20),
    gender VARCHAR(10),
    email VARCHAR(200),
    phone VARCHAR(20),
    address TEXT,
    city VARCHAR(100),
    province VARCHAR(100),
    region VARCHAR(50),
    cohort_year INTEGER,
    department_key INTEGER,
    enrollment_date DATE,
    graduation_date DATE,
    years_enrolled INTEGER,
    status VARCHAR(50),
    -- SCD Type 2 columns
    effective_start_date DATE NOT NULL,
    effective_end_date DATE,
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension: Course
CREATE TABLE IF NOT EXISTS gold.dim_course (
    course_key SERIAL PRIMARY KEY,
    course_id VARCHAR(50) NOT NULL UNIQUE,
    course_name VARCHAR(200),
    course_description TEXT,
    credits INTEGER,
    credit_category VARCHAR(20),
    department_key INTEGER,
    course_level VARCHAR(50),
    course_type VARCHAR(50),
    is_active BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension: Department
CREATE TABLE IF NOT EXISTS gold.dim_department (
    department_key SERIAL PRIMARY KEY,
    department_id VARCHAR(50) NOT NULL UNIQUE,
    department_name VARCHAR(200),
    faculty VARCHAR(200),
    dean_name VARCHAR(100),
    established_year INTEGER,
    department_age INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension: Semester
CREATE TABLE IF NOT EXISTS gold.dim_semester (
    semester_key SERIAL PRIMARY KEY,
    semester_id VARCHAR(50) NOT NULL UNIQUE,
    semester_name VARCHAR(100),
    academic_year INTEGER,
    semester_number INTEGER,
    start_date DATE,
    end_date DATE,
    duration_weeks INTEGER,
    is_current BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension: Instructor
CREATE TABLE IF NOT EXISTS gold.dim_instructor (
    instructor_key SERIAL PRIMARY KEY,
    instructor_id VARCHAR(50) NOT NULL UNIQUE,
    full_name VARCHAR(200),
    email VARCHAR(200),
    department_key INTEGER,
    academic_rank VARCHAR(100),
    degree VARCHAR(100),
    hire_date DATE,
    years_of_service INTEGER,
    is_active BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Fact: Enrollment
CREATE TABLE IF NOT EXISTS gold.fact_enrollment (
    enrollment_key SERIAL PRIMARY KEY,
    enrollment_id VARCHAR(50) NOT NULL UNIQUE,
    student_key INTEGER REFERENCES gold.dim_student(student_key),
    course_key INTEGER REFERENCES gold.dim_course(course_key),
    semester_key INTEGER REFERENCES gold.dim_semester(semester_key),
    enrollment_date_key INTEGER REFERENCES gold.dim_date(date_key),
    midterm_score DECIMAL(5,2),
    final_score DECIMAL(5,2),
    total_score DECIMAL(5,2),
    grade_letter VARCHAR(5),
    gpa_points DECIMAL(3,2),
    is_passed BOOLEAN,
    status VARCHAR(50),
    attendance_count INTEGER,
    absence_count INTEGER,
    attendance_rate DECIMAL(5,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Fact: Tuition
CREATE TABLE IF NOT EXISTS gold.fact_tuition (
    tuition_key SERIAL PRIMARY KEY,
    transaction_id VARCHAR(50) NOT NULL UNIQUE,
    student_key INTEGER REFERENCES gold.dim_student(student_key),
    semester_key INTEGER REFERENCES gold.dim_semester(semester_key),
    payment_date_key INTEGER REFERENCES gold.dim_date(date_key),
    tuition_amount DECIMAL(15,2),
    scholarship_amount DECIMAL(15,2),
    discount_amount DECIMAL(15,2),
    net_amount DECIMAL(15,2),
    paid_amount DECIMAL(15,2),
    remaining_amount DECIMAL(15,2),
    payment_method VARCHAR(50),
    payment_status VARCHAR(50),
    is_fully_paid BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- AGGREGATED TABLES (Data Marts)
-- =============================================================================

-- Academic Performance Summary by Semester
CREATE TABLE IF NOT EXISTS gold.agg_academic_performance_semester (
    id SERIAL PRIMARY KEY,
    semester_key INTEGER,
    department_key INTEGER,
    total_students INTEGER,
    total_enrollments INTEGER,
    avg_gpa DECIMAL(3,2),
    pass_rate DECIMAL(5,2),
    fail_rate DECIMAL(5,2),
    avg_attendance_rate DECIMAL(5,2),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Student Cohort Analysis
CREATE TABLE IF NOT EXISTS gold.agg_cohort_analysis (
    id SERIAL PRIMARY KEY,
    cohort_year INTEGER,
    department_key INTEGER,
    initial_students INTEGER,
    current_students INTEGER,
    graduated_students INTEGER,
    dropped_students INTEGER,
    retention_rate DECIMAL(5,2),
    graduation_rate DECIMAL(5,2),
    avg_years_to_graduate DECIMAL(3,1),
    avg_cumulative_gpa DECIMAL(3,2),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Financial Summary
CREATE TABLE IF NOT EXISTS gold.agg_financial_summary (
    id SERIAL PRIMARY KEY,
    semester_key INTEGER,
    total_tuition_expected DECIMAL(18,2),
    total_scholarships DECIMAL(18,2),
    total_discounts DECIMAL(18,2),
    total_collected DECIMAL(18,2),
    total_outstanding DECIMAL(18,2),
    collection_rate DECIMAL(5,2),
    student_count INTEGER,
    avg_tuition_per_student DECIMAL(15,2),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- AUDIT TABLES
-- =============================================================================

-- ETL Job History
CREATE TABLE IF NOT EXISTS audit.etl_job_history (
    job_id SERIAL PRIMARY KEY,
    job_name VARCHAR(200) NOT NULL,
    dag_id VARCHAR(200),
    run_id VARCHAR(200),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status VARCHAR(50),
    records_processed INTEGER,
    records_inserted INTEGER,
    records_updated INTEGER,
    records_failed INTEGER,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Data Quality Results
CREATE TABLE IF NOT EXISTS audit.data_quality_results (
    result_id SERIAL PRIMARY KEY,
    check_name VARCHAR(200) NOT NULL,
    table_name VARCHAR(200),
    column_name VARCHAR(100),
    check_type VARCHAR(100),
    expected_value TEXT,
    actual_value TEXT,
    is_passed BOOLEAN,
    severity VARCHAR(50),
    check_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    batch_id VARCHAR(50)
);

-- Data Lineage
CREATE TABLE IF NOT EXISTS audit.data_lineage (
    lineage_id SERIAL PRIMARY KEY,
    source_table VARCHAR(200),
    target_table VARCHAR(200),
    transformation_type VARCHAR(100),
    transformation_sql TEXT,
    job_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- INDEXES
-- =============================================================================

-- Bronze layer indexes
CREATE INDEX IF NOT EXISTS idx_bronze_students_ingested ON bronze.raw_students(_ingested_at);
CREATE INDEX IF NOT EXISTS idx_bronze_courses_ingested ON bronze.raw_courses(_ingested_at);
CREATE INDEX IF NOT EXISTS idx_bronze_enrollments_ingested ON bronze.raw_enrollments(_ingested_at);

-- Silver layer indexes
CREATE INDEX IF NOT EXISTS idx_silver_students_id ON silver.stg_students(student_id);
CREATE INDEX IF NOT EXISTS idx_silver_courses_id ON silver.stg_courses(course_id);
CREATE INDEX IF NOT EXISTS idx_silver_enrollments_student ON silver.stg_enrollments(student_id);

-- Gold layer indexes
CREATE INDEX IF NOT EXISTS idx_gold_dim_student_id ON gold.dim_student(student_id);
CREATE INDEX IF NOT EXISTS idx_gold_dim_student_current ON gold.dim_student(is_current);
CREATE INDEX IF NOT EXISTS idx_gold_dim_date_full ON gold.dim_date(full_date);
CREATE INDEX IF NOT EXISTS idx_gold_fact_enrollment_student ON gold.fact_enrollment(student_key);
CREATE INDEX IF NOT EXISTS idx_gold_fact_enrollment_course ON gold.fact_enrollment(course_key);
CREATE INDEX IF NOT EXISTS idx_gold_fact_enrollment_semester ON gold.fact_enrollment(semester_key);
CREATE INDEX IF NOT EXISTS idx_gold_fact_tuition_student ON gold.fact_tuition(student_key);
CREATE INDEX IF NOT EXISTS idx_gold_fact_tuition_semester ON gold.fact_tuition(semester_key);

-- =============================================================================
-- GRANTS (adjust roles as needed)
-- =============================================================================

-- Create roles
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dw_admin') THEN
        CREATE ROLE dw_admin;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dw_developer') THEN
        CREATE ROLE dw_developer;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dw_analyst') THEN
        CREATE ROLE dw_analyst;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dw_readonly') THEN
        CREATE ROLE dw_readonly;
    END IF;
END
$$;

-- Grant permissions
GRANT ALL PRIVILEGES ON SCHEMA bronze, silver, gold, staging, audit TO dw_admin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA bronze, silver, gold, staging, audit TO dw_admin;

GRANT USAGE ON SCHEMA bronze, silver, gold, staging, audit TO dw_developer;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA bronze, silver, gold, staging TO dw_developer;
GRANT SELECT ON ALL TABLES IN SCHEMA audit TO dw_developer;

GRANT USAGE ON SCHEMA gold TO dw_analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO dw_analyst;

GRANT USAGE ON SCHEMA gold TO dw_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO dw_readonly;

-- =============================================================================
-- SEED DATA - Date Dimension
-- =============================================================================

-- Populate date dimension (2020-2030)
INSERT INTO gold.dim_date (date_key, full_date, year, quarter, month, month_name, week, day, day_name, day_of_week, is_weekend, is_holiday, fiscal_year, fiscal_quarter)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INTEGER as date_key,
    d as full_date,
    EXTRACT(YEAR FROM d)::INTEGER as year,
    EXTRACT(QUARTER FROM d)::INTEGER as quarter,
    EXTRACT(MONTH FROM d)::INTEGER as month,
    TO_CHAR(d, 'Month') as month_name,
    EXTRACT(WEEK FROM d)::INTEGER as week,
    EXTRACT(DAY FROM d)::INTEGER as day,
    TO_CHAR(d, 'Day') as day_name,
    EXTRACT(DOW FROM d)::INTEGER as day_of_week,
    EXTRACT(DOW FROM d) IN (0, 6) as is_weekend,
    FALSE as is_holiday,
    CASE WHEN EXTRACT(MONTH FROM d) >= 7 THEN EXTRACT(YEAR FROM d)::INTEGER ELSE EXTRACT(YEAR FROM d)::INTEGER - 1 END as fiscal_year,
    CASE 
        WHEN EXTRACT(MONTH FROM d) BETWEEN 7 AND 9 THEN 1
        WHEN EXTRACT(MONTH FROM d) BETWEEN 10 AND 12 THEN 2
        WHEN EXTRACT(MONTH FROM d) BETWEEN 1 AND 3 THEN 3
        ELSE 4
    END as fiscal_quarter
FROM generate_series('2020-01-01'::DATE, '2030-12-31'::DATE, '1 day'::INTERVAL) d
ON CONFLICT (date_key) DO NOTHING;

COMMIT;
