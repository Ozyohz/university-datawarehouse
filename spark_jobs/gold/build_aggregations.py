"""
Gold Layer - Build Aggregations
================================
Builds summary/aggregated tables for reporting.
"""

import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, avg, count, countDistinct, sum, round
)


def create_spark_session():
    """Create and configure Spark session"""
    return SparkSession.builder \
        .appName("Gold - Build Aggregations") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


def build_academic_performance_summary(spark, jdbc_url, properties):
    """Build academic performance summary by semester and department"""
    print("Building academic performance summary...")
    
    # Read fact_enrollment
    fact_enrollment = spark.read.jdbc(
        url=jdbc_url,
        table="gold.fact_enrollment",
        properties=properties
    )
    
    # Read dim_student to get department_key
    dim_student = spark.read.jdbc(
        url=jdbc_url,
        table="(SELECT student_key, department_key FROM gold.dim_student WHERE is_current = TRUE) AS dim",
        properties=properties
    )
    
    # Join and aggregate
    df_joined = fact_enrollment.join(dim_student, on="student_key", how="inner")
    
    df_agg = df_joined.groupBy("semester_key", "department_key").agg(
        countDistinct("student_key").alias("total_students"),
        count("enrollment_id").alias("total_enrollments"),
        round(avg("gpa_points"), 2).alias("avg_gpa"),
        round(sum(when(col("is_passed") == True, 1).otherwise(0)) * 100 / count("*"), 2).alias("pass_rate"),
        round(sum(when(col("is_passed") == False, 1).otherwise(0)) * 100 / count("*"), 2).alias("fail_rate"),
        round(avg("attendance_rate"), 2).alias("avg_attendance_rate")
    )
    
    # Add metadata
    df_final = df_agg.withColumn("updated_at", current_timestamp())
    
    # Write to database
    df_final.write.jdbc(
        url=jdbc_url,
        table="gold.agg_academic_performance_semester",
        mode="overwrite",
        properties=properties
    )
    
    print("Academic performance summary built successfully")


def build_financial_summary(spark, jdbc_url, properties):
    """Build financial summary by semester"""
    print("Building financial summary...")
    
    # Read fact_tuition
    fact_tuition = spark.read.jdbc(
        url=jdbc_url,
        table="gold.fact_tuition",
        properties=properties
    )
    
    # Aggregate
    df_agg = fact_tuition.groupBy("semester_key").agg(
        sum("tuition_amount").alias("total_tuition_expected"),
        sum("scholarship_amount").alias("total_scholarships"),
        sum("discount_amount").alias("total_discounts"),
        sum("paid_amount").alias("total_collected"),
        sum("remaining_amount").alias("total_outstanding"),
        round(sum("paid_amount") * 100 / sum("net_amount"), 2).alias("collection_rate"),
        countDistinct("student_key").alias("student_count"),
        round(avg("tuition_amount"), 2).alias("avg_tuition_per_student")
    )
    
    # Add metadata
    df_final = df_agg.withColumn("updated_at", current_timestamp())
    
    # Write to database
    df_final.write.jdbc(
        url=jdbc_url,
        table="gold.agg_financial_summary",
        mode="overwrite",
        properties=properties
    )
    
    print("Financial summary built successfully")


def main():
    """Main function to build aggregations"""
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
        print("Starting Gold aggregations build")
        
        # Build summaries
        build_academic_performance_summary(spark, jdbc_url, properties)
        build_financial_summary(spark, jdbc_url, properties)
        
        print("Gold aggregations build completed successfully")
        
    except Exception as e:
        print(f"Error building aggregations: {str(e)}")
        raise
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
