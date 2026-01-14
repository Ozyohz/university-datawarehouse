"""
Gold Layer - Build Fact Enrollment
===================================
Builds the fact_enrollment table joining with all dimension tables.
"""

import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, when, coalesce
)


def create_spark_session():
    """Create and configure Spark session"""
    return SparkSession.builder \
        .appName("Gold - Build Fact Enrollment") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


def read_silver_enrollments(spark, jdbc_url, properties):
    """Read enrollment data from silver layer"""
    query = """
        (SELECT 
            enrollment_id,
            student_id,
            course_id,
            semester_id,
            enrollment_date,
            midterm_score,
            final_score,
            total_score,
            grade_letter,
            gpa_points,
            status,
            attendance_count,
            absence_count,
            attendance_rate
         FROM silver.stg_enrollments
         WHERE is_valid = TRUE
        ) AS silver_enrollments
    """
    return spark.read.jdbc(
        url=jdbc_url,
        table=query,
        properties=properties
    )


def lookup_student_key(df, spark, jdbc_url, properties):
    """Lookup student_key from dim_student (current records only)"""
    dim_student = spark.read.jdbc(
        url=jdbc_url,
        table="(SELECT student_key, student_id FROM gold.dim_student WHERE is_current = TRUE) AS dim",
        properties=properties
    )
    return df.join(dim_student, on="student_id", how="left")


def lookup_course_key(df, spark, jdbc_url, properties):
    """Lookup course_key from dim_course"""
    dim_course = spark.read.jdbc(
        url=jdbc_url,
        table="(SELECT course_key, course_id FROM gold.dim_course) AS dim",
        properties=properties
    )
    return df.join(dim_course, on="course_id", how="left")


def lookup_semester_key(df, spark, jdbc_url, properties):
    """Lookup semester_key from dim_semester"""
    dim_semester = spark.read.jdbc(
        url=jdbc_url,
        table="(SELECT semester_key, semester_id FROM gold.dim_semester) AS dim",
        properties=properties
    )
    return df.join(dim_semester, on="semester_id", how="left")


def lookup_date_key(df, spark, jdbc_url, properties):
    """Lookup date_key from dim_date"""
    dim_date = spark.read.jdbc(
        url=jdbc_url,
        table="(SELECT date_key, full_date FROM gold.dim_date) AS dim",
        properties=properties
    )
    # Join on enrollment_date
    df = df.join(
        dim_date.withColumnRenamed("date_key", "enrollment_date_key"),
        df.enrollment_date == dim_date.full_date,
        how="left"
    ).drop("full_date")
    
    return df


def calculate_derived_metrics(df):
    """Calculate derived metrics for the fact table"""
    return df \
        .withColumn(
            "is_passed",
            when(col("total_score") >= 5.0, True).otherwise(False)
        ) \
        .withColumn(
            "gpa_points",
            when(col("gpa_points").isNull(),
                 when(col("total_score") >= 8.5, 4.0)
                 .when(col("total_score") >= 7.0, 3.0)
                 .when(col("total_score") >= 5.5, 2.0)
                 .when(col("total_score") >= 4.0, 1.0)
                 .otherwise(0.0)
            ).otherwise(col("gpa_points"))
        ) \
        .withColumn(
            "attendance_rate",
            when(col("attendance_rate").isNull(),
                 when(
                     (col("attendance_count") + col("absence_count")) > 0,
                     col("attendance_count") * 100 / (col("attendance_count") + col("absence_count"))
                 ).otherwise(lit(None))
            ).otherwise(col("attendance_rate"))
        )


def write_fact_table(df, jdbc_url, properties, mode="overwrite"):
    """Write fact table to PostgreSQL"""
    # Select final columns
    final_columns = [
        "enrollment_id",
        "student_key",
        "course_key",
        "semester_key",
        "enrollment_date_key",
        "midterm_score",
        "final_score",
        "total_score",
        "grade_letter",
        "gpa_points",
        "is_passed",
        "status",
        "attendance_count",
        "absence_count",
        "attendance_rate"
    ]
    
    df_to_write = df.select(*final_columns) \
        .withColumn("created_at", current_timestamp())
    
    df_to_write.write.jdbc(
        url=jdbc_url,
        table="gold.fact_enrollment",
        mode=mode,
        properties=properties
    )


def main():
    """Main function to build fact_enrollment"""
    # Configuration
    pg_host = os.getenv("DW_HOST", "postgres")
    pg_port = os.getenv("DW_PORT", "5432")
    pg_database = os.getenv("DW_DATABASE", "university_dw")
    pg_user = os.getenv("DW_USER", "datawarehouse")
    pg_password = os.getenv("DW_PASSWORD", "datawarehouse")
    
    jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_database}"
    properties = {
        "user": pg_user,
        "password": pg_password,
        "driver": "org.postgresql.Driver"
    }
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        print("Starting fact_enrollment build")
        
        # Read from silver
        df_silver = read_silver_enrollments(spark, jdbc_url, properties)
        silver_count = df_silver.count()
        print(f"Records from silver: {silver_count}")
        
        # Lookup dimension keys
        df_with_keys = df_silver \
            .transform(lambda df: lookup_student_key(df, spark, jdbc_url, properties)) \
            .transform(lambda df: lookup_course_key(df, spark, jdbc_url, properties)) \
            .transform(lambda df: lookup_semester_key(df, spark, jdbc_url, properties)) \
            .transform(lambda df: lookup_date_key(df, spark, jdbc_url, properties))
        
        # Calculate derived metrics
        df_final = calculate_derived_metrics(df_with_keys)
        
        # Check for missing dimension keys
        missing_student = df_final.filter(col("student_key").isNull()).count()
        missing_course = df_final.filter(col("course_key").isNull()).count()
        missing_semester = df_final.filter(col("semester_key").isNull()).count()
        
        print(f"Records with missing student_key: {missing_student}")
        print(f"Records with missing course_key: {missing_course}")
        print(f"Records with missing semester_key: {missing_semester}")
        
        # Filter out records with missing dimension keys
        df_valid = df_final.filter(
            col("student_key").isNotNull() &
            col("course_key").isNotNull() &
            col("semester_key").isNotNull()
        )
        
        valid_count = df_valid.count()
        print(f"Valid records to write: {valid_count}")
        
        # Write to fact table
        write_fact_table(df_valid, jdbc_url, properties)
        
        print("fact_enrollment build completed successfully")
        
    except Exception as e:
        print(f"Error building fact_enrollment: {str(e)}")
        raise
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
