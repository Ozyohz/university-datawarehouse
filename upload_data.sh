#!/bin/bash
set -e

# Create raw-data bucket
docker-compose run --rm --entrypoint /bin/sh minio-init -c "\
mc alias set myminio http://minio:9000 minioadmin minioadmin123; \
mc mb myminio/raw-data --ignore-existing; \
exit 0"

# Upload data
# We mount sample_data to /data in the container
docker-compose run --rm -v $(pwd)/sample_data:/sample_data --entrypoint /bin/sh minio-init -c "\
mc alias set myminio http://minio:9000 minioadmin minioadmin123; \
mc cp /sample_data/students.csv myminio/raw-data/students/students.csv; \
mc cp /sample_data/courses.csv myminio/raw-data/courses/courses.csv; \
mc cp /sample_data/enrollments.csv myminio/raw-data/enrollments/enrollments.csv; \
mc cp /sample_data/semesters.csv myminio/raw-data/semesters/semesters.csv; \
mc cp /sample_data/departments.csv myminio/raw-data/departments/departments.csv; \
# Tuition file assumption? ingest_tuition.py likely expects tuition.csv
# User didn't have tuition.csv open, but let's check input list or assume it's there
# If not, I'll check ingest_tuition.py
ls /sample_data
exit 0"
