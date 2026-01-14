"""
Silver Layer - Student Data Transformation
===========================================
Cleanses and transforms student data from bronze to silver layer.
"""

import os
from datetime import datetime, date
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, when, trim, upper, lower,
    regexp_replace, concat, coalesce, to_date, year, 
    datediff, current_date, array, collect_list
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DateType, BooleanType, ArrayType
)


def create_spark_session():
    """Create and configure Spark session"""
    return SparkSession.builder \
        .appName("Silver - Transform Students") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


def read_from_bronze(spark, source_path):
    """Read data from bronze layer"""
    return spark.read.format("delta").load(source_path)


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
        .withColumn("first_name", trim(col("first_name"))) \
        .withColumn("last_name", trim(col("last_name"))) \
        .withColumn("email", lower(trim(col("email")))) \
        .withColumn("city", trim(col("city"))) \
        .withColumn("province", trim(col("province"))) \
        .withColumn("phone", regexp_replace(col("phone"), r"[^\d+]", ""))


def standardize_gender(df):
    """Standardize gender values"""
    return df.withColumn(
        "gender",
        when(upper(col("gender")).isin(["M", "MALE", "NAM"]), "Male")
        .when(upper(col("gender")).isin(["F", "FEMALE", "NỮ", "NU"]), "Female")
        .otherwise("Other")
    )


def standardize_status(df):
    """Standardize student status"""
    return df.withColumn(
        "status",
        when(upper(col("status")).isin(["ACTIVE", "ENROLLED", "ĐANG HỌC"]), "Active")
        .when(upper(col("status")).isin(["GRADUATED", "TỐT NGHIỆP"]), "Graduated")
        .when(upper(col("status")).isin(["DROPPED", "BỎ HỌC", "THÔI HỌC"]), "Dropped")
        .when(upper(col("status")).isin(["SUSPENDED", "TẠM NGHỈ"]), "Suspended")
        .otherwise("Unknown")
    )


def create_full_name(df):
    """Create full name from first and last name"""
    return df.withColumn(
        "full_name",
        concat(col("first_name"), lit(" "), col("last_name"))
    )


def parse_dates(df):
    """Parse and validate date columns"""
    return df \
        .withColumn(
            "date_of_birth",
            to_date(col("date_of_birth"), "yyyy-MM-dd")
        ) \
        .withColumn(
            "enrollment_date",
            to_date(col("enrollment_date"), "yyyy-MM-dd")
        )


def validate_data(df):
    """Validate data and flag invalid records"""
    validation_errors = []
    
    # Check for required fields
    df = df.withColumn(
        "is_valid",
        when(
            col("student_id").isNotNull() &
            col("full_name").isNotNull() &
            col("email").isNotNull() &
            col("email").rlike(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"),
            True
        ).otherwise(False)
    )
    
    # Collect validation errors
    df = df.withColumn(
        "validation_errors",
        when(col("student_id").isNull(), array(lit("Missing student_id")))
        .when(col("email").isNull(), array(lit("Missing email")))
        .when(
            ~col("email").rlike(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"),
            array(lit("Invalid email format"))
        )
        .otherwise(array())
    )
    
    return df


def deduplicate(df, key_columns):
    """Remove duplicates based on key columns, keeping the latest record"""
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number, desc
    
    window = Window.partitionBy(key_columns).orderBy(desc("_ingested_at"))
    
    df_with_row_num = df.withColumn("_row_num", row_number().over(window))
    df_deduped = df_with_row_num.filter(col("_row_num") == 1).drop("_row_num")
    
    return df_deduped


def add_derived_columns(df):
    """Add derived/computed columns"""
    return df \
        .withColumn(
            "graduation_date",
            when(col("status") == "Graduated", col("enrollment_date"))
            .otherwise(lit(None))
        )


def write_to_silver(df, target_path, mode="overwrite"):
    """Write data to silver layer using merge for incremental updates"""
    df.write \
        .format("delta") \
        .mode(mode) \
        .save(target_path)


def write_to_postgres(df, jdbc_url, table_name, properties, mode="overwrite"):
    """Write data to PostgreSQL silver table"""
    # Select only the columns we need for silver
    silver_columns = [
        "student_id", "full_name", "date_of_birth", "gender",
        "email", "phone", "address", "city", "province",
        "cohort_year", "department_id", "enrollment_date",
        "graduation_date", "status", "is_valid", "validation_errors"
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
    # Configuration
    bronze_path = os.getenv("BRONZE_PATH", "s3a://bronze/students/")
    silver_path = os.getenv("SILVER_PATH", "s3a://silver/students/")
    
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
        print("Starting student transformation: Bronze → Silver")
        
        # Read from bronze
        # df_bronze = read_from_bronze(spark, bronze_path)
        df_bronze = read_from_postgres(spark, jdbc_url, "bronze.raw_students", properties)
        
        bronze_count = df_bronze.count()
        print(f"Records read from bronze: {bronze_count}")
        
        # Apply transformations
        df_transformed = df_bronze \
            .transform(clean_string_columns) \
            .transform(create_full_name) \
            .transform(standardize_gender) \
            .transform(standardize_status) \
            .transform(parse_dates) \
            .transform(add_derived_columns) \
            .transform(validate_data)
        
        # Deduplicate
        df_deduped = deduplicate(df_transformed, ["student_id"])
        
        silver_count = df_deduped.count()
        valid_count = df_deduped.filter(col("is_valid") == True).count()
        invalid_count = silver_count - valid_count
        
        print(f"Records after transformation: {silver_count}")
        print(f"Valid records: {valid_count}")
        print(f"Invalid records: {invalid_count}")
        
        # Write to silver layer
        # print(f"Writing to Silver Delta Lake: {silver_path}")
        # write_to_silver(df_deduped, silver_path)
        
        # Write to PostgreSQL
        print("Writing to PostgreSQL silver.stg_students")
        write_to_postgres(df_deduped, jdbc_url, "silver.stg_students", properties)
        
        print("Student transformation completed successfully")
        
    except Exception as e:
        print(f"Error during transformation: {str(e)}")
        raise
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
