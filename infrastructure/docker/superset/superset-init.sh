#!/bin/bash
set -e

# Initialize Superset
echo "Initializing Superset..."

# Wait for database
echo "Waiting for database..."
sleep 10

# Run database migrations
superset db upgrade

# Create admin user if not exists
superset fab create-admin \
    --username "${SUPERSET_ADMIN_USER:-admin}" \
    --firstname Admin \
    --lastname User \
    --email "${SUPERSET_ADMIN_EMAIL:-admin@localhost}" \
    --password "${SUPERSET_ADMIN_PASSWORD:-admin}" || true

# Initialize Superset
superset init

# Start Superset
echo "Starting Superset..."
exec superset run -h 0.0.0.0 -p 8088 --with-threads --reload
