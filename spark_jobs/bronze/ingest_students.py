"""
Bronze Layer - Student Data Ingestion
======================================
Ingests student data from source systems into the bronze layer.
"""

import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, input_file_name,
    to_date, trim, upper, lower, regexp_replace
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DateType
)


def create_spark_session():
    """Create and configure Spark session"""
    return SparkSession.builder \
        .appName("Bronze - Ingest Students") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


def get_source_schema():
    """Define the expected schema for student data"""
    return StructType([
        StructField("student_id", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("date_of_birth", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("province", StringType(), True),
        StructField("cohort_year", IntegerType(), True),
        StructField("department_id", StringType(), True),
        StructField("enrollment_date", StringType(), True),
        StructField("status", StringType(), True),
    ])


def ingest_from_database(spark, jdbc_url, table_name, properties):
    """Ingest data from source database"""
    df = spark.read.jdbc(
        url=jdbc_url,
        table=table_name,
        properties=properties
    )
    return df


def ingest_from_files(spark, source_path, file_format="csv"):
    """Ingest data from files (CSV, JSON, Parquet)"""
    schema = get_source_schema()
    
    if file_format == "csv":
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .schema(schema) \
            .csv(source_path)
    elif file_format == "json":
        df = spark.read \
            .schema(schema) \
            .json(source_path)
    elif file_format == "parquet":
        df = spark.read.parquet(source_path)
    else:
        raise ValueError(f"Unsupported file format: {file_format}")
    
    return df


def add_metadata_columns(df, batch_id):
    """Add technical metadata columns"""
    return df \
        .withColumn("_source_file", input_file_name()) \
        .withColumn("_ingested_at", current_timestamp()) \
        .withColumn("_batch_id", lit(batch_id))


def write_to_bronze(df, target_path, mode="append"):
    """Write data to bronze layer"""
    df.write \
        .format("delta") \
        .mode(mode) \
        .partitionBy("_batch_id") \
        .save(target_path)


def write_to_postgres(df, jdbc_url, table_name, properties, mode="append"):
    """Write data to PostgreSQL bronze table"""
    df.write \
        .jdbc(
            url=jdbc_url,
            table=table_name,
            mode=mode,
            properties=properties
        )


def main():
    """Main ingestion function"""
    # Configuration
    batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # MinIO/S3 paths
    source_path = os.getenv("SOURCE_PATH", "s3a://raw-data/students/")
    bronze_path = os.getenv("BRONZE_PATH", "s3a://bronze/students/")
    
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
        print(f"Starting student ingestion - Batch ID: {batch_id}")
        
        # Read from source (file-based for this example)
        df = ingest_from_files(spark, source_path, file_format="csv")
        
        # Add metadata columns
        df_with_metadata = add_metadata_columns(df, batch_id)
        
        # Count records
        record_count = df_with_metadata.count()
        print(f"Records to ingest: {record_count}")
        
        # Write to MinIO (Delta Lake)
        print(f"Writing to Bronze Delta Lake: {bronze_path}")
        write_to_bronze(df_with_metadata, bronze_path)
        
        # Also write to PostgreSQL bronze table
        print("Writing to PostgreSQL bronze.raw_students")
        write_to_postgres(
            df_with_metadata, 
            jdbc_url, 
            "bronze.raw_students", 
            properties
        )
        
        print(f"Successfully ingested {record_count} student records")
        
    except Exception as e:
        print(f"Error during ingestion: {str(e)}")
        raise
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
