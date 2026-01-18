"""
Silver Layer - Student Data Transformation (Refactored)
======================================================
Cleanses and transforms student data from bronze to silver layer.
Uses standardized patterns from dwh_best_practices.
"""

import sys
import os

# Add project root to path to import common utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.spark_utils import (
    get_spark_session, read_postgres_table, write_postgres_table,
    clean_strings, normalize_email, phone_cleaning
)
from pyspark.sql.functions import (
    col, lit, current_timestamp, when, upper, concat, to_date, array
)

def standardize_gender(df):
    """Standardize gender values using business rules"""
    return df.withColumn(
        "gender",
        when(upper(col("gender")).isin(["M", "MALE", "NAM"]), "Male")
        .when(upper(col("gender")).isin(["F", "FEMALE", "NỮ", "NU"]), "Female")
        .otherwise("Other")
    )

def standardize_status(df):
    """Standardize student enrollment status"""
    status_map = {
        "Active": ["ACTIVE", "ENROLLED", "ĐANG HỌC"],
        "Graduated": ["GRADUATED", "TỐT NGHIỆP"],
        "Dropped": ["DROPPED", "BỎ HỌC", "THÔI HỌC"],
        "Suspended": ["SUSPENDED", "TẠM NGHỈ"]
    }
    
    expr = when(upper(col("status")).isin(status_map["Active"]), "Active")
    for standard, aliases in list(status_map.items())[1:]:
        expr = expr.when(upper(col("status")).isin(aliases), standard)
    
    return df.withColumn("status", expr.otherwise("Unknown"))

def validate_student_data(df):
    """Validate data quality and flag records"""
    # Define validation rules
    valid_email_regex = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    
    df = df.withColumn(
        "is_valid",
        col("student_id").isNotNull() & 
        col("email").rlike(valid_email_regex)
    )
    
    # Collect error messages
    df = df.withColumn(
        "validation_errors",
        when(col("student_id").isNull(), array(lit("Missing student_id")))
        .when(~col("email").rlike(valid_email_regex), array(lit("Invalid email format")))
        .otherwise(array())
    )
    return df

def deduplicate_students(df):
    """Keep latest record per student_id using windowing"""
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number, desc
    
    window = Window.partitionBy("student_id").orderBy(desc("_ingested_at"))
    return df.withColumn("_rn", row_number().over(window)) \
             .filter(col("_rn") == 1) \
             .drop("_rn")

def main():
    spark = get_spark_session("Silver - Student Refactored")
    
    try:
        print(">>> Starting Student Transformation [Refactored]")
        
        # 1. Read Raw Data
        df_raw = read_postgres_table(spark, "bronze.raw_students")
        print(f"Read {df_raw.count()} raw records")

        # 2. Transformation Pipeline
        df_silver = df_raw \
            .transform(lambda df: clean_strings(df, ["first_name", "last_name", "city", "province"])) \
            .withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name"))) \
            .transform(normalize_email) \
            .transform(phone_cleaning) \
            .transform(standardize_gender) \
            .transform(standardize_status) \
            .withColumn("date_of_birth", to_date(col("date_of_birth"))) \
            .withColumn("enrollment_date", to_date(col("enrollment_date"))) \
            .transform(validate_student_data) \
            .transform(deduplicate_students)

        # 3. Final selection and Write
        silver_columns = [
            "student_id", "full_name", "date_of_birth", "gender",
            "email", "phone", "address", "city", "province",
            "cohort_year", "department_id", "enrollment_date",
            "status", "is_valid", "validation_errors"
        ]
        
        df_final = df_silver.select(*silver_columns) \
            .withColumn("updated_at", current_timestamp())

        print(f"Writing {df_final.count()} records to silver.stg_students")
        write_postgres_table(df_final, "silver.stg_students")
        
        print(">>> Transformation completed successfully.")

    except Exception as e:
        print(f"!!! Error in transformation: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
