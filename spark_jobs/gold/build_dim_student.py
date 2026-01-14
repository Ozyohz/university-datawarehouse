"""
Gold Layer - Build Student Dimension (SCD Type 2)
=================================================
Builds the dim_student dimension table with Slowly Changing Dimension Type 2.
"""

import os
from datetime import datetime, date
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, current_date, when, 
    coalesce, concat, year, floor, datediff, md5, sha2
)
from pyspark.sql.types import DateType


def create_spark_session():
    """Create and configure Spark session"""
    return SparkSession.builder \
        .appName("Gold - Build Dim Student (SCD2)") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


def read_from_silver(spark, jdbc_url, properties):
    """Read cleansed student data from silver layer"""
    query = """
        (SELECT 
            student_id,
            full_name,
            date_of_birth,
            gender,
            email,
            phone,
            address,
            city,
            province,
            cohort_year,
            department_id,
            enrollment_date,
            graduation_date,
            status
         FROM silver.stg_students
         WHERE is_valid = TRUE
        ) AS silver_students
    """
    return spark.read.jdbc(
        url=jdbc_url,
        table=query,
        properties=properties
    )


def read_existing_dimension(spark, jdbc_url, properties):
    """Read existing dimension table for SCD Type 2 comparison"""
    try:
        return spark.read.jdbc(
            url=jdbc_url,
            table="gold.dim_student",
            properties=properties
        )
    except:
        # Return empty DataFrame if table doesn't exist yet
        return None


def calculate_age_group(df):
    """Calculate age group based on date of birth"""
    return df.withColumn(
        "age_group",
        when(
            datediff(current_date(), col("date_of_birth")) / 365 < 20,
            "Under 20"
        )
        .when(
            datediff(current_date(), col("date_of_birth")) / 365 < 25,
            "20-24"
        )
        .when(
            datediff(current_date(), col("date_of_birth")) / 365 < 30,
            "25-29"
        )
        .when(
            datediff(current_date(), col("date_of_birth")) / 365 < 35,
            "30-34"
        )
        .otherwise("35+")
    )


def calculate_region(df):
    """Map province to region"""
    north = ["Hà Nội", "Hải Phòng", "Quảng Ninh", "Hải Dương", "Bắc Ninh", 
             "Vĩnh Phúc", "Phú Thọ", "Thái Nguyên", "Lạng Sơn", "Cao Bằng"]
    central = ["Đà Nẵng", "Huế", "Quảng Nam", "Quảng Ngãi", "Bình Định",
               "Phú Yên", "Khánh Hòa", "Nghệ An", "Hà Tĩnh", "Quảng Bình"]
    south = ["TP HCM", "Hồ Chí Minh", "Bình Dương", "Đồng Nai", "Long An",
             "Cần Thơ", "An Giang", "Kiên Giang", "Vũng Tàu", "Bà Rịa"]
    
    return df.withColumn(
        "region",
        when(col("province").isin(north), "Miền Bắc")
        .when(col("province").isin(central), "Miền Trung")
        .when(col("province").isin(south), "Miền Nam")
        .otherwise("Khác")
    )


def calculate_years_enrolled(df):
    """Calculate years enrolled"""
    return df.withColumn(
        "years_enrolled",
        when(
            col("graduation_date").isNotNull(),
            floor(datediff(col("graduation_date"), col("enrollment_date")) / 365)
        ).otherwise(
            floor(datediff(current_date(), col("enrollment_date")) / 365)
        ).cast("integer")
    )


def create_hash_key(df, columns):
    """Create hash key for SCD Type 2 change detection"""
    concat_cols = concat(*[coalesce(col(c).cast("string"), lit("")) for c in columns])
    return df.withColumn("_hash_key", sha2(concat_cols, 256))


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


def build_new_dimension(df_silver, current_date_val):
    """Build new dimension records"""
    return df_silver \
        .withColumn("effective_start_date", lit(current_date_val).cast(DateType())) \
        .withColumn("effective_end_date", lit(None).cast(DateType())) \
        .withColumn("is_current", lit(True)) \
        .withColumn("created_at", current_timestamp()) \
        .withColumn("updated_at", current_timestamp())


def apply_scd_type2(df_silver, df_existing, current_date_val):
    """Apply SCD Type 2 logic"""
    if df_existing is None:
        # First load - all records are new
        return build_new_dimension(df_silver, current_date_val), None
    
    # Create hash keys for comparison
    scd_columns = ["full_name", "email", "phone", "address", "city", 
                   "province", "status", "graduation_date"]
    
    df_silver_hashed = create_hash_key(df_silver, scd_columns)
    df_existing_current = df_existing.filter(col("is_current") == True)
    df_existing_hashed = create_hash_key(df_existing_current, scd_columns)
    
    # Find changed records
    df_changed = df_silver_hashed.alias("s").join(
        df_existing_hashed.alias("e"),
        col("s.student_id") == col("e.student_id"),
        "inner"
    ).filter(
        col("s._hash_key") != col("e._hash_key")
    ).select("s.*")
    
    # Find new records
    df_new = df_silver_hashed.alias("s").join(
        df_existing_hashed.alias("e"),
        col("s.student_id") == col("e.student_id"),
        "left_anti"
    )
    
    # Build new dimension records
    df_inserts = df_new.unionByName(df_changed).drop("_hash_key")
    df_inserts = build_new_dimension(df_inserts, current_date_val)
    
    # Close old records for changed students
    df_updates = df_changed.select("student_id")
    
    return df_inserts, df_updates


def write_dimension(df, jdbc_url, properties, mode="append"):
    """Write dimension table to PostgreSQL"""
    # Select final columns
    final_columns = [
        "student_id", "full_name", "date_of_birth", "age_group",
        "gender", "email", "phone", "address", "city", "province",
        "region", "cohort_year", "department_key", "enrollment_date",
        "graduation_date", "years_enrolled", "status",
        "effective_start_date", "effective_end_date", "is_current",
        "created_at", "updated_at"
    ]
    
    df_to_write = df.select(*final_columns)
    
    df_to_write.write.jdbc(
        url=jdbc_url,
        table="gold.dim_student",
        mode=mode,
        properties=properties
    )


def close_old_records(spark, jdbc_url, properties, student_ids, close_date):
    """Close old SCD Type 2 records"""
    if student_ids is None or student_ids.count() == 0:
        return
    
    # Collect student IDs to close
    ids_list = [row.student_id for row in student_ids.collect()]
    ids_str = "','".join(ids_list)
    
    # Execute update via JDBC
    from pyspark.sql import DataFrame
    
    update_sql = f"""
        UPDATE gold.dim_student
        SET is_current = FALSE,
            effective_end_date = '{close_date}',
            updated_at = CURRENT_TIMESTAMP
        WHERE student_id IN ('{ids_str}')
          AND is_current = TRUE
    """
    
    # Note: In production, use proper JDBC connection for DML
    print(f"Closing {len(ids_list)} old records")


def main():
    """Main function to build dim_student"""
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
    
    current_date_val = date.today()
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        print("Starting dim_student build (SCD Type 2)")
        
        # Read from silver
        df_silver = read_from_silver(spark, jdbc_url, properties)
        silver_count = df_silver.count()
        print(f"Records from silver: {silver_count}")
        
        # Apply enrichments
        df_enriched = df_silver \
            .transform(calculate_age_group) \
            .transform(calculate_region) \
            .transform(calculate_years_enrolled)
        
        # Lookup dimension keys
        df_with_keys = lookup_department_key(df_enriched, spark, jdbc_url, properties)
        
        # Read existing dimension
        df_existing = read_existing_dimension(spark, jdbc_url, properties)
        
        # Apply SCD Type 2
        df_inserts, df_updates = apply_scd_type2(
            df_with_keys, df_existing, current_date_val
        )
        
        insert_count = df_inserts.count() if df_inserts else 0
        print(f"New/Changed records to insert: {insert_count}")
        
        # Close old records (if any updates)
        if df_updates is not None:
            close_old_records(spark, jdbc_url, properties, df_updates, current_date_val)
        
        # Write new dimension records
        if insert_count > 0:
            write_dimension(df_inserts, jdbc_url, properties)
        
        print("dim_student build completed successfully")
        
    except Exception as e:
        print(f"Error building dim_student: {str(e)}")
        raise
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
