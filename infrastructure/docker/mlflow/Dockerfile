# MLflow Docker Image
FROM python:3.11-slim

# Install dependencies
RUN pip install --no-cache-dir \
    mlflow>=2.10.0 \
    psycopg2-binary>=2.9.0 \
    boto3>=1.34.0 \
    sqlalchemy>=2.0.0

# Create mlflow user
RUN useradd -m -s /bin/bash mlflow

USER mlflow
WORKDIR /home/mlflow

EXPOSE 5000

CMD ["mlflow", "server", "--host", "0.0.0.0", "--port", "5000"]
