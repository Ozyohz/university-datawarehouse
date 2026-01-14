"""
Silver Layer - Course Data Transformation
==========================================
Cleanses and transforms course data from bronze to silver layer.
"""

import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, when, trim, upper, array
)


def create_spark_session():
    """Create and configure Spark session"""
    return SparkSession.builder \
        .appName("Silver - Transform Courses") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


def read_from_postgres(spark, jdbc_url, table_name, properties):
    """Read latest data from PostgreSQL bronze table"""
    query = f"""
        (SELECT * FROM {table_name} 
         WHERE _ingested_at >= CURRENT_DATE - INTERVAL '1 day'
        ) AS latest_data
    """
    return spark.read.jdbc(
        url=jdbc_url,
        table=query,
        properties=properties
    )


def clean_string_columns(df):
    """Clean and standardize string columns"""
    return df \
        .withColumn("course_name", trim(col("course_name"))) \
        .withColumn("course_description", trim(col("course_description"))) \
        .withColumn("course_level", trim(col("course_level"))) \
        .withColumn("course_type", trim(col("course_type")))


def validate_data(df):
    """Validate data and flag invalid records"""
    return df.withColumn(
        "is_valid",
        when(
            col("course_id").isNotNull() &
            col("course_name").isNotNull() &
            col("credits").isNotNull(),
            True
        ).otherwise(False)
    ).withColumn(
        "validation_errors",
        when(col("course_id").isNull(), array(lit("Missing course_id")))
        .when(col("course_name").isNull(), array(lit("Missing course_name")))
        .when(col("credits").isNull(), array(lit("Missing credits")))
        .otherwise(array())
    )


def deduplicate(df, key_columns):
    """Remove duplicates based on key columns, keeping the latest record"""
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number, desc
    
    window = Window.partitionBy(key_columns).orderBy(desc("_ingested_at"))
    
    df_with_row_num = df.withColumn("_row_num", row_number().over(window))
    df_deduped = df_with_row_num.filter(col("_row_num") == 1).drop("_row_num")
    
    return df_deduped


def write_to_postgres(df, jdbc_url, table_name, properties, mode="overwrite"):
    """Write data to PostgreSQL silver table"""
    silver_columns = [
        "course_id", "course_name", "course_description", "credits",
        "department_id", "course_level", "course_type", "is_active",
        "is_valid", "validation_errors"
    ]
    
    df_to_write = df.select(*silver_columns) \
        .withColumn("created_at", current_timestamp()) \
        .withColumn("updated_at", current_timestamp())
    
    df_to_write.write \
        .jdbc(
            url=jdbc_url,
            table=table_name,
            mode=mode,
            properties=properties
        )


def main():
    """Main transformation function"""
    # PostgreSQL configuration
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
        print("Starting course transformation: Bronze → Silver")
        
        # Read from bronze
        df_bronze = read_from_postgres(spark, jdbc_url, "bronze.raw_courses", properties)
        
        # Apply transformations
        df_transformed = df_bronze \
            .transform(clean_string_columns) \
            .transform(validate_data)
        
        # Deduplicate
        df_deduped = deduplicate(df_transformed, ["course_id"])
        
        # Write to PostgreSQL
        print("Writing to PostgreSQL silver.stg_courses")
        write_to_postgres(df_deduped, jdbc_url, "silver.stg_courses", properties)
        
        print("Course transformation completed successfully")
        
    except Exception as e:
        print(f"Error during transformation: {str(e)}")
        raise
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
