"""
University Data Warehouse - Main ETL Pipeline DAG
=================================================
This DAG orchestrates the full ETL pipeline from Bronze to Gold layer.
"""

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

# =============================================================================
# Default Arguments
# =============================================================================
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['data-team@university.edu'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# =============================================================================
# DAG Definition
# =============================================================================
with DAG(
    dag_id='etl_full_pipeline',
    default_args=default_args,
    description='Full ETL pipeline: Bronze → Silver → Gold',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'daily', 'production'],
    max_active_runs=1,
) as dag:

    # =========================================================================
    # Start & End Tasks
    # =========================================================================
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    # =========================================================================
    # Bronze Layer - Data Ingestion
    # =========================================================================
    with TaskGroup(group_id='bronze_ingestion') as bronze_group:
        
        ingest_students = SparkSubmitOperator(
            task_id='ingest_students',
            application='/opt/airflow/spark_jobs/bronze/ingest_students.py',
            conn_id='spark_default',
            verbose=True,
            packages='io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.1',
            conf={
                'spark.sql.extensions': 'io.delta.sql.DeltaSparkSessionExtension',
                'spark.sql.catalog.spark_catalog': 'org.apache.spark.sql.delta.catalog.DeltaCatalog',
                'spark.hadoop.fs.s3a.endpoint': f"http://{os.getenv('MINIO_ENDPOINT', 'minio:9000')}",
                'spark.hadoop.fs.s3a.access.key': os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
                'spark.hadoop.fs.s3a.secret.key': os.getenv('MINIO_SECRET_KEY', 'minioadmin123'),
                'spark.hadoop.fs.s3a.path.style.access': 'true',
                'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
                'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
            },
        )

        ingest_courses = SparkSubmitOperator(
            task_id='ingest_courses',
            application='/opt/airflow/spark_jobs/bronze/ingest_courses.py',
            conn_id='spark_default',
            verbose=True,
            packages='io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.1',
            conf={
                'spark.hadoop.fs.s3a.endpoint': f"http://{os.getenv('MINIO_ENDPOINT', 'minio:9000')}",
                'spark.hadoop.fs.s3a.access.key': os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
                'spark.hadoop.fs.s3a.secret.key': os.getenv('MINIO_SECRET_KEY', 'minioadmin123'),
                'spark.hadoop.fs.s3a.path.style.access': 'true',
                'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
                'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
            },
        )

        ingest_enrollments = SparkSubmitOperator(
            task_id='ingest_enrollments',
            application='/opt/airflow/spark_jobs/bronze/ingest_enrollments.py',
            conn_id='spark_default',
            verbose=True,
            packages='io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.1',
            conf={
                'spark.hadoop.fs.s3a.endpoint': f"http://{os.getenv('MINIO_ENDPOINT', 'minio:9000')}",
                'spark.hadoop.fs.s3a.access.key': os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
                'spark.hadoop.fs.s3a.secret.key': os.getenv('MINIO_SECRET_KEY', 'minioadmin123'),
                'spark.hadoop.fs.s3a.path.style.access': 'true',
                'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
                'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
            },
        )

        ingest_tuition = SparkSubmitOperator(
            task_id='ingest_tuition',
            application='/opt/airflow/spark_jobs/bronze/ingest_tuition.py',
            conn_id='spark_default',
            verbose=True,
            packages='io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.1',
            conf={
                'spark.hadoop.fs.s3a.endpoint': f"http://{os.getenv('MINIO_ENDPOINT', 'minio:9000')}",
                'spark.hadoop.fs.s3a.access.key': os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
                'spark.hadoop.fs.s3a.secret.key': os.getenv('MINIO_SECRET_KEY', 'minioadmin123'),
                'spark.hadoop.fs.s3a.path.style.access': 'true',
                'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
                'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
            },
        )

        ingest_semesters = SparkSubmitOperator(
            task_id='ingest_semesters',
            application='/opt/airflow/spark_jobs/bronze/ingest_semesters.py',
            conn_id='spark_default',
            verbose=True,
            packages='io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.1',
            conf={
                'spark.hadoop.fs.s3a.endpoint': f"http://{os.getenv('MINIO_ENDPOINT', 'minio:9000')}",
                'spark.hadoop.fs.s3a.access.key': os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
                'spark.hadoop.fs.s3a.secret.key': os.getenv('MINIO_SECRET_KEY', 'minioadmin123'),
                'spark.hadoop.fs.s3a.path.style.access': 'true',
                'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
                'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
            },
        )

        ingest_departments = SparkSubmitOperator(
            task_id='ingest_departments',
            application='/opt/airflow/spark_jobs/bronze/ingest_departments.py',
            conn_id='spark_default',
            verbose=True,
            packages='io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.1',
            conf={
                'spark.hadoop.fs.s3a.endpoint': f"http://{os.getenv('MINIO_ENDPOINT', 'minio:9000')}",
                'spark.hadoop.fs.s3a.access.key': os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
                'spark.hadoop.fs.s3a.secret.key': os.getenv('MINIO_SECRET_KEY', 'minioadmin123'),
                'spark.hadoop.fs.s3a.path.style.access': 'true',
                'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
                'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
            },
        )

        [ingest_students, ingest_courses, ingest_enrollments, 
         ingest_tuition, ingest_semesters, ingest_departments]

    # =========================================================================
    # Bronze Quality Check
    # =========================================================================
    bronze_quality_check = BashOperator(
        task_id='bronze_quality_check',
        bash_command='great_expectations checkpoint run bronze_checkpoint',
    )

    # =========================================================================
    # Silver Layer - Data Cleansing & Transformation
    # =========================================================================
    with TaskGroup(group_id='silver_transformation') as silver_group:

        transform_students = SparkSubmitOperator(
            task_id='transform_students',
            application='/opt/airflow/spark_jobs/silver/transform_students.py',
            conn_id='spark_default',
            verbose=True,
            packages='io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.1',
            conf={
                'spark.hadoop.fs.s3a.endpoint': f"http://{os.getenv('MINIO_ENDPOINT', 'minio:9000')}",
                'spark.hadoop.fs.s3a.access.key': os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
                'spark.hadoop.fs.s3a.secret.key': os.getenv('MINIO_SECRET_KEY', 'minioadmin123'),
                'spark.hadoop.fs.s3a.path.style.access': 'true',
                'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
                'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
            },
        )

        transform_courses = SparkSubmitOperator(
            task_id='transform_courses',
            application='/opt/airflow/spark_jobs/silver/transform_courses.py',
            conn_id='spark_default',
            verbose=True,
            packages='io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.1',
            conf={
                'spark.hadoop.fs.s3a.endpoint': f"http://{os.getenv('MINIO_ENDPOINT', 'minio:9000')}",
                'spark.hadoop.fs.s3a.access.key': os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
                'spark.hadoop.fs.s3a.secret.key': os.getenv('MINIO_SECRET_KEY', 'minioadmin123'),
                'spark.hadoop.fs.s3a.path.style.access': 'true',
                'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
                'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
            },
        )

        transform_enrollments = SparkSubmitOperator(
            task_id='transform_enrollments',
            application='/opt/airflow/spark_jobs/silver/transform_enrollments.py',
            conn_id='spark_default',
            verbose=True,
            packages='io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.1',
            conf={
                'spark.hadoop.fs.s3a.endpoint': f"http://{os.getenv('MINIO_ENDPOINT', 'minio:9000')}",
                'spark.hadoop.fs.s3a.access.key': os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
                'spark.hadoop.fs.s3a.secret.key': os.getenv('MINIO_SECRET_KEY', 'minioadmin123'),
                'spark.hadoop.fs.s3a.path.style.access': 'true',
                'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
                'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
            },
        )

        transform_tuition = SparkSubmitOperator(
            task_id='transform_tuition',
            application='/opt/airflow/spark_jobs/silver/transform_tuition.py',
            conn_id='spark_default',
            verbose=True,
            packages='io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.1',
            conf={
                'spark.hadoop.fs.s3a.endpoint': f"http://{os.getenv('MINIO_ENDPOINT', 'minio:9000')}",
                'spark.hadoop.fs.s3a.access.key': os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
                'spark.hadoop.fs.s3a.secret.key': os.getenv('MINIO_SECRET_KEY', 'minioadmin123'),
                'spark.hadoop.fs.s3a.path.style.access': 'true',
                'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
                'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
            },
        )

        [transform_students, transform_courses, transform_enrollments, transform_tuition]

    # =========================================================================
    # Silver Quality Check
    # =========================================================================
    silver_quality_check = BashOperator(
        task_id='silver_quality_check',
        bash_command='great_expectations checkpoint run silver_checkpoint',
    )

    # =========================================================================
    # Gold Layer - Dimension & Fact Tables
    # =========================================================================
    with TaskGroup(group_id='gold_build') as gold_group:

        # Build Dimensions
        build_dim_department = SparkSubmitOperator(
            task_id='build_dim_department',
            application='/opt/airflow/spark_jobs/gold/build_dim_department.py',
            conn_id='spark_default',
            verbose=True,
            packages='io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.1',
            conf={
                'spark.hadoop.fs.s3a.endpoint': f"http://{os.getenv('MINIO_ENDPOINT', 'minio:9000')}",
                'spark.hadoop.fs.s3a.access.key': os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
                'spark.hadoop.fs.s3a.secret.key': os.getenv('MINIO_SECRET_KEY', 'minioadmin123'),
                'spark.hadoop.fs.s3a.path.style.access': 'true',
                'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
                'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
            },
        )

        build_dim_semester = SparkSubmitOperator(
            task_id='build_dim_semester',
            application='/opt/airflow/spark_jobs/gold/build_dim_semester.py',
            conn_id='spark_default',
            verbose=True,
            packages='io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.1',
            conf={
                'spark.hadoop.fs.s3a.endpoint': f"http://{os.getenv('MINIO_ENDPOINT', 'minio:9000')}",
                'spark.hadoop.fs.s3a.access.key': os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
                'spark.hadoop.fs.s3a.secret.key': os.getenv('MINIO_SECRET_KEY', 'minioadmin123'),
                'spark.hadoop.fs.s3a.path.style.access': 'true',
                'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
                'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
            },
        )

        build_dim_course = SparkSubmitOperator(
            task_id='build_dim_course',
            application='/opt/airflow/spark_jobs/gold/build_dim_course.py',
            conn_id='spark_default',
            verbose=True,
            packages='io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.1',
            conf={
                'spark.hadoop.fs.s3a.endpoint': f"http://{os.getenv('MINIO_ENDPOINT', 'minio:9000')}",
                'spark.hadoop.fs.s3a.access.key': os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
                'spark.hadoop.fs.s3a.secret.key': os.getenv('MINIO_SECRET_KEY', 'minioadmin123'),
                'spark.hadoop.fs.s3a.path.style.access': 'true',
                'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
                'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
            },
        )

        build_dim_student = SparkSubmitOperator(
            task_id='build_dim_student',
            application='/opt/airflow/spark_jobs/gold/build_dim_student.py',
            conn_id='spark_default',
            verbose=True,
            packages='io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.1',
            conf={
                'spark.hadoop.fs.s3a.endpoint': f"http://{os.getenv('MINIO_ENDPOINT', 'minio:9000')}",
                'spark.hadoop.fs.s3a.access.key': os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
                'spark.hadoop.fs.s3a.secret.key': os.getenv('MINIO_SECRET_KEY', 'minioadmin123'),
                'spark.hadoop.fs.s3a.path.style.access': 'true',
                'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
                'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
            },
        )

        # Build Facts (depends on dimensions)
        build_fact_enrollment = SparkSubmitOperator(
            task_id='build_fact_enrollment',
            application='/opt/airflow/spark_jobs/gold/build_fact_enrollment.py',
            conn_id='spark_default',
            verbose=True,
            packages='io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.1',
            conf={
                'spark.hadoop.fs.s3a.endpoint': f"http://{os.getenv('MINIO_ENDPOINT', 'minio:9000')}",
                'spark.hadoop.fs.s3a.access.key': os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
                'spark.hadoop.fs.s3a.secret.key': os.getenv('MINIO_SECRET_KEY', 'minioadmin123'),
                'spark.hadoop.fs.s3a.path.style.access': 'true',
                'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
                'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
            },
        )

        build_fact_tuition = SparkSubmitOperator(
            task_id='build_fact_tuition',
            application='/opt/airflow/spark_jobs/gold/build_fact_tuition.py',
            conn_id='spark_default',
            verbose=True,
            packages='io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.1',
            conf={
                'spark.hadoop.fs.s3a.endpoint': f"http://{os.getenv('MINIO_ENDPOINT', 'minio:9000')}",
                'spark.hadoop.fs.s3a.access.key': os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
                'spark.hadoop.fs.s3a.secret.key': os.getenv('MINIO_SECRET_KEY', 'minioadmin123'),
                'spark.hadoop.fs.s3a.path.style.access': 'true',
                'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
                'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
            },
        )

        # Build Aggregations
        build_aggregations = SparkSubmitOperator(
            task_id='build_aggregations',
            application='/opt/airflow/spark_jobs/gold/build_aggregations.py',
            conn_id='spark_default',
            verbose=True,
            packages='io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.1',
            conf={
                'spark.hadoop.fs.s3a.endpoint': f"http://{os.getenv('MINIO_ENDPOINT', 'minio:9000')}",
                'spark.hadoop.fs.s3a.access.key': os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
                'spark.hadoop.fs.s3a.secret.key': os.getenv('MINIO_SECRET_KEY', 'minioadmin123'),
                'spark.hadoop.fs.s3a.path.style.access': 'true',
                'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
                'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
            },
        )

        # Dependencies within gold layer
        [build_dim_department, build_dim_semester] >> build_dim_course
        build_dim_course >> build_dim_student
        build_dim_student >> [build_fact_enrollment, build_fact_tuition]
        [build_fact_enrollment, build_fact_tuition] >> build_aggregations

    # =========================================================================
    # Gold Quality Check
    # =========================================================================
    gold_quality_check = BashOperator(
        task_id='gold_quality_check',
        bash_command='great_expectations checkpoint run gold_checkpoint',
    )

    # =========================================================================
    # dbt Run (alternative transformation)
    # =========================================================================
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/dbt && dbt run --target prod',
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/dbt && dbt test --target prod',
    )

    # =========================================================================
    # Notification
    # =========================================================================
    def send_success_notification(**context):
        """Send success notification via Slack/Email"""
        from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
        
        execution_date = context['execution_date']
        message = f"""
        ✅ *ETL Pipeline Completed Successfully*
        📅 Execution Date: {execution_date}
        🔗 DAG: etl_full_pipeline
        """
        # Log the message (Slack integration optional)
        print(message)
        return message

    success_notification = PythonOperator(
        task_id='success_notification',
        python_callable=send_success_notification,
        trigger_rule='all_success',
    )

    # =========================================================================
    # Task Dependencies
    # =========================================================================
    start >> bronze_group >> bronze_quality_check
    bronze_quality_check >> silver_group >> silver_quality_check
    silver_quality_check >> gold_group >> gold_quality_check
    gold_quality_check >> dbt_run >> dbt_test >> success_notification >> end
