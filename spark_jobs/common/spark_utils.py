"""
Spark Utils - Common Utilities for Data ETL
===========================================
Contains reusable functions for Spark session management, 
database I/O, and data cleaning.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, lower, regexp_replace, to_date, when, concat, lit
)

def get_spark_session(app_name="Data ETL"):
    """Create and configure a standardized Spark session"""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

def get_jdbc_properties():
    """Get database connection properties from environment"""
    pg_user = os.getenv("DW_USER", "datawarehouse")
    pg_password = os.getenv("DW_PASSWORD", "datawarehouse")
    
    return {
        "user": pg_user,
        "password": pg_password,
        "driver": "org.postgresql.Driver"
    }

def get_jdbc_url():
    """Construct JDBC URL from environment"""
    pg_host = os.getenv("DW_HOST", "postgres")
    pg_port = os.getenv("DW_PORT", "5432")
    pg_database = os.getenv("DW_DATABASE", "university_dw")
    return f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_database}"

def read_postgres_table(spark, table_name, filter_condition=None):
    """Read a table from PostgreSQL"""
    url = get_jdbc_url()
    properties = get_jdbc_properties()
    
    if filter_condition:
        query = f"(SELECT * FROM {table_name} WHERE {filter_condition}) AS filtered_data"
        return spark.read.jdbc(url=url, table=query, properties=properties)
    
    return spark.read.jdbc(url=url, table=table_name, properties=properties)

def write_postgres_table(df, table_name, mode="overwrite"):
    """Write DataFrame to PostgreSQL table"""
    url = get_jdbc_url()
    properties = get_jdbc_properties()
    
    df.write.jdbc(url=url, table=table_name, mode=mode, properties=properties)

# Data Cleaning Mixins
def clean_strings(df, columns):
    """Trim and nullify empty strings in specified columns"""
    for c in columns:
        df = df.withColumn(c, trim(col(c)))
        df = df.withColumn(c, when(col(c) == "", lit(None)).otherwise(col(c)))
    return df

def normalize_email(df, email_col="email"):
    """Lowercase and trim email"""
    return df.withColumn(email_col, lower(trim(col(email_col))))

def phone_cleaning(df, phone_col="phone"):
    """Remove non-digit characters except +"""
    return df.withColumn(phone_col, regexp_replace(col(phone_col), r"[^\d+]", ""))
