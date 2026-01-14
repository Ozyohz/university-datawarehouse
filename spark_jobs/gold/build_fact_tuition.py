"""
Gold Layer - Build Fact Tuition
================================
Builds the fact_tuition table joining with all dimension tables.
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
        .appName("Gold - Build Fact Tuition") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


def read_silver_tuition(spark, jdbc_url, properties):
    """Read tuition data from silver layer"""
    query = """
        (SELECT 
            transaction_id,
            student_id,
            semester_id,
            tuition_amount,
            scholarship_amount,
            discount_amount,
            net_amount,
            paid_amount,
            balance_amount,
            payment_date,
            payment_method,
            status
         FROM silver.stg_tuition
         WHERE is_valid = TRUE
        ) AS silver_tuition
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
    # Join on payment_date
    df = df.join(
        dim_date.withColumnRenamed("date_key", "payment_date_key"),
        df.payment_date == dim_date.full_date,
        how="left"
    ).drop("full_date")
    
    return df


def calculate_derived_metrics(df):
    """Calculate derived metrics for the fact table"""
    return df \
        .withColumn(
            "remaining_amount",
            col("balance_amount")
        ) \
        .withColumn(
            "payment_status",
            col("status")
        ) \
        .withColumn(
            "is_fully_paid",
            when(col("balance_amount") <= 0, True).otherwise(False)
        )


def write_fact_table(df, jdbc_url, properties, mode="overwrite"):
    """Write fact table to PostgreSQL"""
    # Select final columns
    final_columns = [
        "transaction_id",
        "student_key",
        "semester_key",
        "payment_date_key",
        "tuition_amount",
        "scholarship_amount",
        "discount_amount",
        "net_amount",
        "paid_amount",
        "remaining_amount",
        "payment_method",
        "payment_status",
        "is_fully_paid"
    ]
    
    df_to_write = df.select(*final_columns) \
        .withColumn("created_at", current_timestamp())
    
    df_to_write.write.jdbc(
        url=jdbc_url,
        table="gold.fact_tuition",
        mode=mode,
        properties=properties
    )


def main():
    """Main function to build fact_tuition"""
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
        print("Starting fact_tuition build")
        
        # Read from silver
        df_silver = read_silver_tuition(spark, jdbc_url, properties)
        silver_count = df_silver.count()
        print(f"Records from silver: {silver_count}")
        
        # Lookup dimension keys
        df_with_keys = df_silver \
            .transform(lambda df: lookup_student_key(df, spark, jdbc_url, properties)) \
            .transform(lambda df: lookup_semester_key(df, spark, jdbc_url, properties)) \
            .transform(lambda df: lookup_date_key(df, spark, jdbc_url, properties))
        
        # Calculate derived metrics
        df_final = calculate_derived_metrics(df_with_keys)
        
        # Filter out records with missing dimension keys
        df_valid = df_final.filter(
            col("student_key").isNotNull() &
            col("semester_key").isNotNull()
        )
        
        valid_count = df_valid.count()
        print(f"Valid records to write: {valid_count}")
        
        # Write to fact table
        write_fact_table(df_valid, jdbc_url, properties)
        
        print("fact_tuition build completed successfully")
        
    except Exception as e:
        print(f"Error building fact_tuition: {str(e)}")
        raise
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
