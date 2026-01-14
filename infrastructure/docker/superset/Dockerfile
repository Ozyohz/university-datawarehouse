# Apache Superset Docker Image
FROM apache/superset:3.1.0

USER root

# Install additional database drivers
RUN pip install --no-cache-dir \
    psycopg2-binary>=2.9.0 \
    sqlalchemy>=2.0.0 \
    gevent>=23.9.0

# Copy bootstrap script
COPY superset-init.sh /app/superset-init.sh
RUN chmod +x /app/superset-init.sh

USER superset

# Set entrypoint
ENTRYPOINT ["/app/superset-init.sh"]
