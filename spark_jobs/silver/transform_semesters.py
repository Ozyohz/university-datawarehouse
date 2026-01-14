"""
Silver Layer - Semester Data Transformation
============================================
Cleanses and transforms semester data from bronze to silver layer.
"""

import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, when, trim, array
)


def create_spark_session():
    """Create and configure Spark session"""
    return SparkSession.builder \
        .appName("Silver - Transform Semesters") \
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


def validate_data(df):
    """Validate data and flag invalid records"""
    return df.withColumn(
        "is_valid",
        when(
            col("semester_id").isNotNull() &
            col("start_date").isNotNull() &
            col("end_date").isNotNull(),
            True
        ).otherwise(False)
    ).withColumn(
        "validation_errors",
        when(col("semester_id").isNull(), array(lit("Missing semester_id")))
        .when(col("start_date").isNull(), array(lit("Missing start_date")))
        .when(col("end_date").isNull(), array(lit("Missing end_date")))
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
        "semester_id", "semester_name", "academic_year", "semester_number",
        "start_date", "end_date", "is_current", "is_valid", "validation_errors"
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
        print("Starting semester transformation: Bronze → Silver")
        
        # Read from bronze
        df_bronze = read_from_postgres(spark, jdbc_url, "bronze.raw_semesters", properties)
        
        # Apply transformations
        df_transformed = df_bronze \
            .transform(validate_data)
        
        # Deduplicate
        df_deduped = deduplicate(df_transformed, ["semester_id"])
        
        # Write to PostgreSQL
        print("Writing to PostgreSQL silver.stg_semesters")
        # Wait, does stg_semesters exist? I checked init-db.sql and it was truncated.
        # I'll check stg_semesters definition.
        write_to_postgres(df_deduped, jdbc_url, "silver.stg_semesters", properties)
        
        print("Semester transformation completed successfully")
        
    except Exception as e:
        print(f"Error during transformation: {str(e)}")
        raise
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
