"""
Gold Layer - Build Course Dimension
====================================
Builds the dim_course dimension table.
"""

import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp


def create_spark_session():
    """Create and configure Spark session"""
    return SparkSession.builder \
        .appName("Gold - Build Dim Course") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


def read_from_silver(spark, jdbc_url, properties):
    """Read cleansed course data from silver layer"""
    query = """
        (SELECT 
            course_id,
            course_name,
            course_description,
            credits,
            department_id,
            course_level,
            course_type,
            is_active
         FROM silver.stg_courses
         WHERE is_valid = TRUE
        ) AS silver_courses
    """
    return spark.read.jdbc(
        url=jdbc_url,
        table=query,
        properties=properties
    )


def lookup_department_key(df, spark, jdbc_url, properties):
    """Lookup department_key from dim_department"""
    try:
        dim_dept = spark.read.jdbc(
            url=jdbc_url,
            table="gold.dim_department",
            properties=properties
        ).select(
            col("department_id"),
            col("department_key")
        )
        
        return df.join(dim_dept, on="department_id", how="left")
    except:
        return df.withColumn("department_key", lit(None).cast("integer"))


def write_dimension(df, jdbc_url, properties, mode="overwrite"):
    """Write dimension table to PostgreSQL"""
    final_columns = [
        "course_id", "course_name", "course_description", "credits",
        "department_key", "course_level", "course_type", "is_active"
    ]
    
    df_to_write = df.select(*final_columns) \
        .withColumn("created_at", current_timestamp()) \
        .withColumn("updated_at", current_timestamp())
    
    df_to_write.write.jdbc(
        url=jdbc_url,
        table="gold.dim_course",
        mode=mode,
        properties=properties
    )


def main():
    """Main function to build dim_course"""
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
        print("Starting dim_course build")
        
        # Read from silver
        df_silver = read_from_silver(spark, jdbc_url, properties)
        
        # Lookup keys
        df_with_keys = lookup_department_key(df_silver, spark, jdbc_url, properties)
        
        # Write dimension
        write_dimension(df_with_keys, jdbc_url, properties)
        
        print("dim_course build completed successfully")
        
    except Exception as e:
        print(f"Error building dim_course: {str(e)}")
        raise
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
