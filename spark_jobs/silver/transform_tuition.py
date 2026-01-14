"""
Silver Layer - Tuition Data Transformation
===========================================
Cleanses and transforms tuition data from bronze to silver layer.
"""

import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, when, trim, upper, array, round, coalesce
)


def create_spark_session():
    """Create and configure Spark session"""
    return SparkSession.builder \
        .appName("Silver - Transform Tuition") \
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


def calculate_financials(df):
    """Calculate derived financial columns"""
    # Calculate total discount
    df = df.withColumn(
        "total_discount",
        coalesce(col("scholarship_amount"), lit(0)) + coalesce(col("discount_amount"), lit(0))
    )
    
    # Calculate net amount
    df = df.withColumn(
        "net_amount",
        col("tuition_amount") - col("total_discount")
    )
    
    # Calculate balance amount
    df = df.withColumn(
        "balance_amount",
        col("net_amount") - col("paid_amount")
    )
    
    # Calculate payment ratio
    df = df.withColumn(
        "payment_ratio",
        round(col("paid_amount") / col("net_amount") * 100, 2)
    )
    
    return df


def validate_data(df):
    """Validate data and flag invalid records"""
    return df.withColumn(
        "is_valid",
        when(
            col("transaction_id").isNotNull() &
            col("student_id").isNotNull() &
            col("net_amount") >= 0,
            True
        ).otherwise(False)
    ).withColumn(
        "validation_errors",
        when(col("transaction_id").isNull(), array(lit("Missing transaction_id")))
        .when(col("student_id").isNull(), array(lit("Missing student_id")))
        .when(col("net_amount") < 0, array(lit("Negative net amount")))
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
        "transaction_id", "student_id", "semester_id", "tuition_amount",
        "scholarship_amount", "discount_amount", "total_discount",
        "net_amount", "paid_amount", "balance_amount", "payment_date",
        "payment_method", "payment_ratio", "status", "is_valid", "validation_errors"
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
        print("Starting tuition transformation: Bronze → Silver")
        
        # Read from bronze
        df_bronze = read_from_postgres(spark, jdbc_url, "bronze.raw_tuition", properties)
        
        # Apply transformations
        df_transformed = df_bronze \
            .transform(calculate_financials) \
            .transform(validate_data)
        
        # Deduplicate
        df_deduped = deduplicate(df_transformed, ["transaction_id"])
        
        # Write to PostgreSQL
        print("Writing to PostgreSQL silver.stg_tuition")
        write_to_postgres(df_deduped, jdbc_url, "silver.stg_tuition", properties)
        
        print("Tuition transformation completed successfully")
        
    except Exception as e:
        print(f"Error during transformation: {str(e)}")
        raise
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
