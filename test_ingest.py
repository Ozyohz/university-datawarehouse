import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, to_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def main():
    spark = SparkSession.builder \
        .appName("Bronze - Ingest Test") \
        .getOrCreate()
    
    source_path = os.getenv("SOURCE_PATH", "/opt/airflow/sample_data/students.csv")
    
    df = spark.read \
        .option("header", "true") \
        .csv(source_path)
    
    df_with_metadata = df.withColumn("_ingested_at", current_timestamp()) \
                         .withColumn("_batch_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S")))
    
    jdbc_url = f"jdbc:postgresql://postgres:5432/university_dw"
    properties = {
        "user": "datawarehouse",
        "password": "your_secure_password_here",
        "driver": "org.postgresql.Driver"
    }
    
    print(f"Ingesting from {source_path} to bronze.raw_students...")
    df_with_metadata.write.jdbc(url=jdbc_url, table="bronze.raw_students", mode="append", properties=properties)
    print("Ingestion successful!")
    spark.stop()

if __name__ == "__main__":
    main()
