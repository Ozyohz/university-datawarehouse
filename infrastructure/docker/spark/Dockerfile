# Apache Spark Docker Image
FROM apache/spark:3.5.0

USER root

# Install Python packages
RUN pip install --no-cache-dir \
    delta-spark>=3.0.0 \
    pandas>=2.1.0 \
    pyarrow>=14.0.0 \
    boto3>=1.34.0 \
    minio>=7.2.0 \
    psycopg2-binary>=2.9.0 \
    great-expectations>=0.18.0 \
    pyyaml>=6.0.0

# Create directories
RUN mkdir -p /opt/spark/jobs /opt/spark/logs /opt/spark/data

# Copy Spark configurations
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

# Set working directory
WORKDIR /opt/spark

# Default command
CMD ["/opt/spark/bin/spark-class", "org.apache.spark.deploy.master.Master"]
