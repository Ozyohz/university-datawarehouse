"""
Gold Layer - Build Semester Dimension
======================================
Builds the dim_semester dimension table.
"""

import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, datediff, floor


def create_spark_session():
    """Create and configure Spark session"""
    return SparkSession.builder \
        .appName("Gold - Build Dim Semester") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


def read_from_silver(spark, jdbc_url, properties):
    """Read cleansed semester data from silver layer"""
    query = """
        (SELECT 
            semester_id,
            semester_name,
            academic_year,
            semester_number,
            start_date,
            end_date,
            is_current
         FROM silver.stg_semesters
         WHERE is_valid = TRUE
        ) AS silver_semesters
    """
    return spark.read.jdbc(
        url=jdbc_url,
        table=query,
        properties=properties
    )


def write_dimension(df, jdbc_url, properties, mode="overwrite"):
    """Write dimension table to PostgreSQL"""
    # Calculate duration in weeks
    df_transformed = df.withColumn(
        "duration_weeks",
        floor(datediff(col("end_date"), col("start_date")) / 7)
    )
    
    final_columns = [
        "semester_id", "semester_name", "academic_year", 
        "semester_number", "start_date", "end_date", 
        "duration_weeks", "is_current"
    ]
    
    df_to_write = df_transformed.select(*final_columns) \
        .withColumn("created_at", current_timestamp()) \
        .withColumn("updated_at", current_timestamp())
    
    df_to_write.write.jdbc(
        url=jdbc_url,
        table="gold.dim_semester",
        mode=mode,
        properties=properties
    )


def main():
    """Main function to build dim_semester"""
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
        print("Starting dim_semester build")
        
        # Read from silver
        df_silver = read_from_silver(spark, jdbc_url, properties)
        
        # Write dimension
        write_dimension(df_silver, jdbc_url, properties)
        
        print("dim_semester build completed successfully")
        
    except Exception as e:
        print(f"Error building dim_semester: {str(e)}")
        raise
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
