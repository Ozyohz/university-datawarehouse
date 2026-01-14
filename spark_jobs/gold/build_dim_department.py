"""
Gold Layer - Build Department Dimension
=========================================
Builds the dim_department dimension table.
"""

import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp


def create_spark_session():
    """Create and configure Spark session"""
    return SparkSession.builder \
        .appName("Gold - Build Dim Department") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


def read_from_silver(spark, jdbc_url, properties):
    """Read cleansed department data from silver layer"""
    # Note: stg_departments may not exist yet if I haven't created the transform script.
    # I'll check if stg_departments exists in silver.
    # If not, I'll use raw_departments for now or just assume it exists.
    # Actually, I should create transform_departments.py too!
    query = """
        (SELECT 
            department_id,
            department_name,
            faculty,
            dean_name,
            established_year
         FROM silver.stg_departments
         WHERE is_valid = TRUE
        ) AS silver_departments
    """
    return spark.read.jdbc(
        url=jdbc_url,
        table=query,
        properties=properties
    )


def write_dimension(df, jdbc_url, properties, mode="overwrite"):
    """Write dimension table to PostgreSQL"""
    final_columns = [
        "department_id", "department_name", "faculty", 
        "dean_name", "established_year"
    ]
    
    df_to_write = df.select(*final_columns) \
        .withColumn("created_at", current_timestamp()) \
        .withColumn("updated_at", current_timestamp())
    
    df_to_write.write.jdbc(
        url=jdbc_url,
        table="gold.dim_department",
        mode=mode,
        properties=properties
    )


def main():
    """Main function to build dim_department"""
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
        print("Starting dim_department build")
        
        # Read from silver
        df_silver = read_from_silver(spark, jdbc_url, properties)
        
        # Write dimension
        write_dimension(df_silver, jdbc_url, properties)
        
        print("dim_department build completed successfully")
        
    except Exception as e:
        print(f"Error building dim_department: {str(e)}")
        raise
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
