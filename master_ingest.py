import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, to_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType

def get_session():
    return SparkSession.builder.appName("MasterIngest").getOrCreate()

def ingest_entity(spark, entity, schema, source_path, table_name):
    print(f">>> Ingesting {entity}...")
    df = spark.read.option("header", "true").schema(schema).csv(source_path)
    
    # Cast necessary columns based on entity
    if entity == "students":
        df = df.withColumn("date_of_birth", to_date(col("date_of_birth"))) \
               .withColumn("enrollment_date", to_date(col("enrollment_date")))
    elif entity == "enrollments":
        df = df.withColumn("enrollment_date", to_date(col("enrollment_date")))
    elif entity == "tuition":
        df = df.withColumn("payment_date", to_date(col("payment_date")))
    
    df_final = df.withColumn("_ingested_at", current_timestamp()) \
                 .withColumn("_batch_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S"))) \
                 .withColumn("_source_file", lit(source_path))

    jdbc_url = "jdbc:postgresql://postgres:5432/university_dw"
    properties = {"user": "datawarehouse", "password": "your_secure_password_here", "driver": "org.postgresql.Driver"}
    
    df_final.write.jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=properties)
    print(f"Done {entity}: {df_final.count()} records.")

def main():
    spark = get_session()
    
    entities = {
        "students": (
            StructType([
                StructField("student_id", StringType()), StructField("first_name", StringType()),
                StructField("last_name", StringType()), StructField("date_of_birth", StringType()),
                StructField("gender", StringType()), StructField("email", StringType()),
                StructField("phone", StringType()), StructField("address", StringType()),
                StructField("city", StringType()), StructField("province", StringType()),
                StructField("cohort_year", IntegerType()), StructField("department_id", StringType()),
                StructField("enrollment_date", StringType()), StructField("status", StringType())
            ]), 
            "/opt/airflow/sample_data/students.csv", "bronze.raw_students"
        ),
        "courses": (
            StructType([
                StructField("course_id", StringType()), StructField("course_name", StringType()),
                StructField("course_description", StringType()), StructField("credits", IntegerType()),
                StructField("department_id", StringType()), StructField("course_level", StringType()),
                StructField("course_type", StringType()), StructField("is_active", StringType())
            ]),
            "/opt/airflow/sample_data/courses.csv", "bronze.raw_courses"
        ),
        "enrollments": (
            StructType([
                StructField("enrollment_id", StringType()), StructField("student_id", StringType()),
                StructField("course_id", StringType()), StructField("semester_id", StringType()),
                StructField("enrollment_date", StringType()), StructField("grade", StringType()),
                StructField("status", StringType())
            ]),
            "/opt/airflow/sample_data/enrollments.csv", "bronze.raw_enrollments"
        ),
        "tuition": (
            StructType([
                StructField("transaction_id", StringType()), StructField("student_id", StringType()),
                StructField("semester_id", StringType()), StructField("tuition_amount", DecimalType(15,2)),
                StructField("scholarship_amount", DecimalType(15,2)), StructField("discount_amount", DecimalType(15,2)),
                StructField("paid_amount", DecimalType(15,2)), StructField("payment_date", StringType()),
                StructField("payment_method", StringType()), StructField("status", StringType())
            ]),
            "/opt/airflow/sample_data/tuition.csv", "bronze.raw_tuition"
        ),
        "semesters": (
            StructType([
                StructField("semester_id", StringType()), StructField("semester_name", StringType()),
                StructField("start_date", StringType()), StructField("end_date", StringType()),
                StructField("academic_year", StringType()), StructField("status", StringType())
            ]),
            "/opt/airflow/sample_data/semesters.csv", "bronze.raw_semesters"
        ),
        "departments": (
            StructType([
                StructField("department_id", StringType()), StructField("department_name", StringType()),
                StructField("description", StringType()), StructField("dean", StringType()),
                StructField("is_active", StringType())
            ]),
            "/opt/airflow/sample_data/departments.csv", "bronze.raw_departments"
        )
    }

    for name, (schema, path, table) in entities.items():
        try:
            ingest_entity(spark, name, schema, path, table)
        except Exception as e:
            print(f"Error ingesting {name}: {e}")

    spark.stop()

if __name__ == "__main__":
    main()
