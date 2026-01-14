# University Data Warehouse - Data Dictionary

## Overview

This document provides detailed descriptions of all tables in the Data Warehouse, organized by layer (Bronze, Silver, Gold).

---

## Bronze Layer (Raw Data)

Raw data as-is from source systems. No transformations applied except adding metadata columns.

### bronze.raw_students

| Column | Type | Description | Source |
|--------|------|-------------|--------|
| id | SERIAL | Auto-increment primary key | System generated |
| student_id | VARCHAR(50) | Unique student identifier | Source system |
| first_name | VARCHAR(100) | Student's first name | Source system |
| last_name | VARCHAR(100) | Student's last name | Source system |
| date_of_birth | DATE | Date of birth | Source system |
| gender | VARCHAR(10) | Gender (M/F/Other) | Source system |
| email | VARCHAR(200) | Email address | Source system |
| phone | VARCHAR(20) | Phone number | Source system |
| address | TEXT | Full address | Source system |
| city | VARCHAR(100) | City | Source system |
| province | VARCHAR(100) | Province | Source system |
| cohort_year | INTEGER | Year of enrollment cohort | Source system |
| department_id | VARCHAR(50) | Department identifier | Source system |
| enrollment_date | DATE | Date of enrollment | Source system |
| status | VARCHAR(50) | Student status | Source system |
| _source_file | VARCHAR(500) | Source file name | ETL metadata |
| _ingested_at | TIMESTAMP | Ingestion timestamp | ETL metadata |
| _batch_id | VARCHAR(50) | Batch identifier | ETL metadata |

### bronze.raw_courses

| Column | Type | Description | Source |
|--------|------|-------------|--------|
| id | SERIAL | Auto-increment primary key | System generated |
| course_id | VARCHAR(50) | Unique course identifier | Source system |
| course_name | VARCHAR(200) | Course name | Source system |
| course_description | TEXT | Course description | Source system |
| credits | INTEGER | Number of credits | Source system |
| department_id | VARCHAR(50) | Department identifier | Source system |
| course_level | VARCHAR(50) | Level (Undergraduate/Graduate) | Source system |
| course_type | VARCHAR(50) | Type (Required/Elective) | Source system |
| is_active | BOOLEAN | Active status | Source system |
| _source_file | VARCHAR(500) | Source file name | ETL metadata |
| _ingested_at | TIMESTAMP | Ingestion timestamp | ETL metadata |
| _batch_id | VARCHAR(50) | Batch identifier | ETL metadata |

### bronze.raw_enrollments

| Column | Type | Description | Source |
|--------|------|-------------|--------|
| id | SERIAL | Auto-increment primary key | System generated |
| enrollment_id | VARCHAR(50) | Unique enrollment identifier | Source system |
| student_id | VARCHAR(50) | Student identifier | Source system |
| course_id | VARCHAR(50) | Course identifier | Source system |
| semester_id | VARCHAR(50) | Semester identifier | Source system |
| enrollment_date | DATE | Date of enrollment | Source system |
| midterm_score | DECIMAL(5,2) | Midterm exam score (0-10) | Source system |
| final_score | DECIMAL(5,2) | Final exam score (0-10) | Source system |
| total_score | DECIMAL(5,2) | Total score (0-10) | Source system |
| grade_letter | VARCHAR(5) | Letter grade | Source system |
| status | VARCHAR(50) | Enrollment status | Source system |
| attendance_count | INTEGER | Number of classes attended | Source system |
| absence_count | INTEGER | Number of absences | Source system |

---

## Silver Layer (Cleaned Data)

Cleansed, validated, and deduplicated data ready for business transformations.

### silver.stg_students

| Column | Type | Description | Transformation |
|--------|------|-------------|----------------|
| student_key | SERIAL | Surrogate key | System generated |
| student_id | VARCHAR(50) | Natural key (unique) | Trimmed |
| full_name | VARCHAR(200) | Full name | Concatenated from first/last |
| date_of_birth | DATE | Date of birth | Parsed, validated |
| gender | VARCHAR(10) | Standardized gender | Standardized (Male/Female/Other) |
| email | VARCHAR(200) | Email address | Lowercase, validated format |
| phone | VARCHAR(20) | Phone number | Cleaned (digits only) |
| address | TEXT | Address | Trimmed |
| city | VARCHAR(100) | City | Trimmed |
| province | VARCHAR(100) | Province | Trimmed |
| cohort_year | INTEGER | Cohort year | Validated range |
| department_id | VARCHAR(50) | Department ID | Trimmed |
| enrollment_date | DATE | Enrollment date | Parsed |
| graduation_date | DATE | Graduation date | Derived if graduated |
| status | VARCHAR(50) | Standardized status | Standardized values |
| is_valid | BOOLEAN | Validation flag | Business rules applied |
| validation_errors | TEXT[] | List of validation errors | Collected errors |
| created_at | TIMESTAMP | Record creation time | System generated |
| updated_at | TIMESTAMP | Last update time | System generated |

### silver.stg_enrollments

| Column | Type | Description | Transformation |
|--------|------|-------------|----------------|
| enrollment_key | SERIAL | Surrogate key | System generated |
| enrollment_id | VARCHAR(50) | Natural key (unique) | Trimmed |
| student_id | VARCHAR(50) | Student identifier | Trimmed |
| course_id | VARCHAR(50) | Course identifier | Trimmed |
| semester_id | VARCHAR(50) | Semester identifier | Trimmed |
| enrollment_date | DATE | Enrollment date | Parsed |
| midterm_score | DECIMAL(5,2) | Midterm score | Validated 0-10 |
| final_score | DECIMAL(5,2) | Final score | Validated 0-10 |
| total_score | DECIMAL(5,2) | Total score | Validated 0-10 |
| grade_letter | VARCHAR(5) | Letter grade | Standardized |
| gpa_points | DECIMAL(3,2) | GPA points (0-4) | Calculated from score |
| status | VARCHAR(50) | Enrollment status | Standardized |
| attendance_count | INTEGER | Attendance count | Validated ≥ 0 |
| absence_count | INTEGER | Absence count | Validated ≥ 0 |
| attendance_rate | DECIMAL(5,2) | Attendance rate % | Calculated |
| is_valid | BOOLEAN | Validation flag | Business rules |
| validation_errors | TEXT[] | Validation errors | Collected errors |

---

## Gold Layer (Business Ready)

Star schema optimized for analytics with dimension and fact tables.

### gold.dim_student (SCD Type 2)

| Column | Type | Description | Notes |
|--------|------|-------------|-------|
| student_key | SERIAL | Surrogate key (PK) | Auto-increment |
| student_id | VARCHAR(50) | Natural key (NK) | Business identifier |
| full_name | VARCHAR(200) | Student full name | |
| date_of_birth | DATE | Date of birth | |
| age_group | VARCHAR(20) | Age category | Derived: Under 20, 20-24, 25-29, 30-34, 35+ |
| gender | VARCHAR(10) | Gender | Male/Female/Other |
| email | VARCHAR(200) | Email address | |
| phone | VARCHAR(20) | Phone number | |
| address | TEXT | Full address | |
| city | VARCHAR(100) | City | |
| province | VARCHAR(100) | Province | |
| region | VARCHAR(50) | Geographic region | Derived: Miền Bắc/Trung/Nam/Khác |
| cohort_year | INTEGER | Enrollment cohort year | |
| department_key | INTEGER | FK to dim_department | |
| enrollment_date | DATE | First enrollment date | |
| graduation_date | DATE | Graduation date | NULL if not graduated |
| years_enrolled | INTEGER | Years in program | Derived |
| status | VARCHAR(50) | Current status | Active/Graduated/Dropped/Suspended |
| effective_start_date | DATE | SCD2 start date | When this version became active |
| effective_end_date | DATE | SCD2 end date | NULL for current record |
| is_current | BOOLEAN | Current record flag | TRUE for latest version |
| created_at | TIMESTAMP | Record creation | |
| updated_at | TIMESTAMP | Last update | |

### gold.dim_course

| Column | Type | Description | Notes |
|--------|------|-------------|-------|
| course_key | SERIAL | Surrogate key (PK) | |
| course_id | VARCHAR(50) | Natural key (NK) | |
| course_name | VARCHAR(200) | Course name | |
| course_description | TEXT | Description | |
| credits | INTEGER | Credit hours | |
| credit_category | VARCHAR(20) | Credit category | Derived: Light (1-2), Standard (3), Heavy (4+) |
| department_key | INTEGER | FK to dim_department | |
| course_level | VARCHAR(50) | Academic level | Undergraduate/Graduate |
| course_type | VARCHAR(50) | Requirement type | Required/Elective |
| is_active | BOOLEAN | Active status | |

### gold.dim_semester

| Column | Type | Description | Notes |
|--------|------|-------------|-------|
| semester_key | SERIAL | Surrogate key (PK) | |
| semester_id | VARCHAR(50) | Natural key (NK) | e.g., SEM2024F |
| semester_name | VARCHAR(100) | Display name | e.g., Fall 2024 |
| academic_year | INTEGER | Academic year | |
| semester_number | INTEGER | Semester in year | 1 (Fall), 2 (Spring), 3 (Summer) |
| start_date | DATE | Semester start | |
| end_date | DATE | Semester end | |
| duration_weeks | INTEGER | Duration in weeks | Derived |
| is_current | BOOLEAN | Current semester | |

### gold.dim_department

| Column | Type | Description | Notes |
|--------|------|-------------|-------|
| department_key | SERIAL | Surrogate key (PK) | |
| department_id | VARCHAR(50) | Natural key (NK) | |
| department_name | VARCHAR(200) | Full name | |
| faculty | VARCHAR(200) | Parent faculty | |
| dean_name | VARCHAR(100) | Current dean | |
| established_year | INTEGER | Year established | |
| department_age | INTEGER | Years in operation | Derived |

### gold.dim_date

| Column | Type | Description | Notes |
|--------|------|-------------|-------|
| date_key | INTEGER | Surrogate key (PK) | Format: YYYYMMDD |
| full_date | DATE | Full date | |
| year | INTEGER | Calendar year | |
| quarter | INTEGER | Quarter (1-4) | |
| month | INTEGER | Month (1-12) | |
| month_name | VARCHAR(20) | Month name | |
| week | INTEGER | Week of year | |
| day | INTEGER | Day of month | |
| day_name | VARCHAR(20) | Day name | |
| day_of_week | INTEGER | Day of week (0-6) | 0=Sunday |
| is_weekend | BOOLEAN | Weekend flag | |
| is_holiday | BOOLEAN | Holiday flag | Needs maintenance |
| fiscal_year | INTEGER | Fiscal year | July start |
| fiscal_quarter | INTEGER | Fiscal quarter | |

### gold.fact_enrollment

| Column | Type | Description | Notes |
|--------|------|-------------|-------|
| enrollment_key | SERIAL | Surrogate key (PK) | |
| enrollment_id | VARCHAR(50) | Natural key | |
| student_key | INTEGER | FK to dim_student | |
| course_key | INTEGER | FK to dim_course | |
| semester_key | INTEGER | FK to dim_semester | |
| enrollment_date_key | INTEGER | FK to dim_date | |
| midterm_score | DECIMAL(5,2) | Midterm score | 0-10 scale |
| final_score | DECIMAL(5,2) | Final score | 0-10 scale |
| total_score | DECIMAL(5,2) | Overall score | 0-10 scale |
| grade_letter | VARCHAR(5) | Letter grade | A, B+, B, C+, C, D, F |
| gpa_points | DECIMAL(3,2) | GPA points | 0-4 scale |
| is_passed | BOOLEAN | Pass status | TRUE if total_score ≥ 5.0 |
| status | VARCHAR(50) | Enrollment status | Completed/In Progress/Withdrawn |
| attendance_count | INTEGER | Classes attended | |
| absence_count | INTEGER | Classes missed | |
| attendance_rate | DECIMAL(5,2) | Attendance % | |

### gold.fact_tuition

| Column | Type | Description | Notes |
|--------|------|-------------|-------|
| tuition_key | SERIAL | Surrogate key (PK) | |
| transaction_id | VARCHAR(50) | Natural key | |
| student_key | INTEGER | FK to dim_student | |
| semester_key | INTEGER | FK to dim_semester | |
| payment_date_key | INTEGER | FK to dim_date | |
| tuition_amount | DECIMAL(15,2) | Total tuition | |
| scholarship_amount | DECIMAL(15,2) | Scholarship deduction | |
| discount_amount | DECIMAL(15,2) | Other discounts | |
| net_amount | DECIMAL(15,2) | Amount due | Derived |
| paid_amount | DECIMAL(15,2) | Amount paid | |
| remaining_amount | DECIMAL(15,2) | Outstanding balance | Derived |
| payment_method | VARCHAR(50) | Payment method | Cash/Bank/Online |
| payment_status | VARCHAR(50) | Payment status | Paid/Partial/Unpaid |
| is_fully_paid | BOOLEAN | Fully paid flag | |

---

## Aggregation Tables

### gold.agg_academic_performance_semester

Pre-aggregated academic metrics by semester and department.

| Column | Type | Description |
|--------|------|-------------|
| semester_key | INTEGER | FK to dim_semester |
| department_key | INTEGER | FK to dim_department |
| total_students | INTEGER | Unique student count |
| total_enrollments | INTEGER | Total enrollment count |
| avg_gpa | DECIMAL(3,2) | Average GPA |
| pass_rate | DECIMAL(5,2) | Pass rate % |
| fail_rate | DECIMAL(5,2) | Fail rate % |
| avg_attendance_rate | DECIMAL(5,2) | Average attendance % |
| grade_a_count | INTEGER | Count of A grades |
| grade_b_count | INTEGER | Count of B grades |
| grade_c_count | INTEGER | Count of C grades |
| grade_d_count | INTEGER | Count of D grades |
| grade_f_count | INTEGER | Count of F grades |

### gold.agg_cohort_analysis

Student cohort retention and graduation metrics.

| Column | Type | Description |
|--------|------|-------------|
| cohort_year | INTEGER | Enrollment year |
| department_key | INTEGER | FK to dim_department |
| initial_students | INTEGER | Starting cohort size |
| current_students | INTEGER | Currently enrolled |
| graduated_students | INTEGER | Total graduated |
| dropped_students | INTEGER | Total dropped/suspended |
| retention_rate | DECIMAL(5,2) | Retention rate % |
| graduation_rate | DECIMAL(5,2) | Graduation rate % |
| avg_years_to_graduate | DECIMAL(3,1) | Average time to graduate |
| avg_cumulative_gpa | DECIMAL(3,2) | Average final GPA |

---

## Business Rules

### Grade Calculation
- GPA Points mapping:
  - A (8.5-10.0): 4.0
  - B+ (7.0-8.4): 3.0
  - C+ (5.5-6.9): 2.0
  - D (4.0-5.4): 1.0
  - F (< 4.0): 0.0

### Status Classification
- **Active**: Currently enrolled
- **Graduated**: Completed program
- **Dropped**: Voluntarily left
- **Suspended**: Temporarily inactive

### Age Groups
- Under 20: < 20 years old
- 20-24: 20-24 years old
- 25-29: 25-29 years old
- 30-34: 30-34 years old
- 35+: 35 years and older

### Region Mapping (Vietnam)
- Miền Bắc (North): Hà Nội, Hải Phòng, Quảng Ninh, etc.
- Miền Trung (Central): Đà Nẵng, Huế, Nghệ An, etc.
- Miền Nam (South): TP HCM, Bình Dương, Đồng Nai, etc.
