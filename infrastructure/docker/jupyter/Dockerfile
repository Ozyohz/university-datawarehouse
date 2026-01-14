# Jupyter Notebook Docker Image
FROM jupyter/pyspark-notebook:latest

USER root

# Install additional packages
RUN pip install --no-cache-dir \
    delta-spark>=3.0.0 \
    pandas>=2.1.0 \
    pyarrow>=14.0.0 \
    boto3>=1.34.0 \
    minio>=7.2.0 \
    psycopg2-binary>=2.9.0 \
    sqlalchemy>=2.0.0 \
    great-expectations>=0.18.0 \
    mlflow>=2.10.0 \
    scikit-learn>=1.4.0 \
    matplotlib>=3.8.0 \
    seaborn>=0.13.0 \
    plotly>=5.18.0

# Create directories
RUN mkdir -p /home/jovyan/notebooks /home/jovyan/data
RUN chown -R jovyan:users /home/jovyan

USER jovyan

WORKDIR /home/jovyan
