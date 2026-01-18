import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, trim, lower, regexp_replace, when, concat, to_date, array

def clean_strings(df, columns):
    for c in columns:
        df = df.withColumn(c, trim(col(c)))
        df = df.withColumn(c, when(col(c) == "", lit(None)).otherwise(col(c)))
    return df

def normalize_email(df, email_col="email"):
    return df.withColumn(email_col, lower(trim(col(email_col))))

def phone_cleaning(df, phone_col="phone"):
    return df.withColumn(phone_col, regexp_replace(col(phone_col), r"[^\d+]", ""))

def get_session():
    return SparkSession.builder.appName("MasterTransform").getOrCreate()

def run_transform(spark, entity, table_in, table_out):
    print(f">>> Transforming {entity}...")
    jdbc_url = "jdbc:postgresql://postgres:5432/university_dw"
    properties = {"user": "datawarehouse", "password": "your_secure_password_here", "driver": "org.postgresql.Driver"}
    
    df = spark.read.jdbc(url=jdbc_url, table=table_in, properties=properties)
    
    if entity == "students":
        df = df.transform(lambda d: clean_strings(d, ["first_name", "last_name", "city", "province"])) \
               .withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name"))) \
               .transform(normalize_email) \
               .transform(phone_cleaning) \
               .withColumn("is_valid", lit(True)) \
               .withColumn("validation_errors", array())
        cols = ["student_id", "full_name", "date_of_birth", "gender", "email", "phone", "address", "city", "province", "cohort_year", "department_id", "enrollment_date", "status", "is_valid", "validation_errors"]
    
    elif entity == "courses":
        df = clean_strings(df, ["course_name", "course_description"]) \
               .withColumn("is_valid", lit(True)) \
               .withColumn("validation_errors", array())
        cols = ["course_id", "course_name", "course_description", "credits", "department_id", "course_level", "course_type", "is_active", "is_valid", "validation_errors"]
    
    elif entity == "enrollments":
        df = df.withColumn("is_valid", lit(True)) \
               .withColumn("validation_errors", array())
        cols = ["enrollment_id", "student_id", "course_id", "semester_id", "enrollment_date", "grade", "status", "is_valid", "validation_errors"]
    
    elif entity == "tuition":
        df = df.withColumn("is_valid", lit(True)) \
               .withColumn("validation_errors", array())
        cols = ["transaction_id", "student_id", "semester_id", "tuition_amount", "scholarship_amount", "discount_amount", "paid_amount", "payment_date", "payment_method", "status", "is_valid", "validation_errors"]
    
    elif entity == "departments":
        df = df.withColumn("is_valid", lit(True)) \
               .withColumn("validation_errors", array())
        cols = ["department_id", "department_name", "faculty", "dean_name", "established_year", "is_valid", "validation_errors"]
    
    elif entity == "semesters":
        df = df.withColumn("is_valid", lit(True)) \
               .withColumn("validation_errors", array())
        cols = ["semester_id", "semester_name", "academic_year", "semester_number", "start_date", "end_date", "is_current", "is_valid", "validation_errors"]

    df_final = df.select(*cols).withColumn("updated_at", current_timestamp())
    df_final.write.jdbc(url=jdbc_url, table=table_out, mode="overwrite", properties=properties)
    print(f"Done {entity}: {df_final.count()} records.")

def main():
    spark = get_session()
    transforms = [
        ("students", "bronze.raw_students", "silver.stg_students"),
        ("courses", "bronze.raw_courses", "silver.stg_courses"),
        ("enrollments", "bronze.raw_enrollments", "silver.stg_enrollments"),
        ("tuition", "bronze.raw_tuition", "silver.stg_tuition"),
        ("departments", "bronze.raw_departments", "silver.stg_departments"),
        ("semesters", "bronze.raw_semesters", "silver.stg_semesters")
    ]
    for e, ti, to in transforms:
        try:
            run_transform(spark, e, ti, to)
        except Exception as ex:
            print(f"Error transforming {e}: {ex}")
    spark.stop()

if __name__ == "__main__":
    main()
