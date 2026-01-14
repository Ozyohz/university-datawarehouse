#!/bin/bash
# =============================================================================
# Create Multiple Databases for PostgreSQL
# =============================================================================

set -e
set -u

function create_user_and_database() {
    local database=$1
    echo "Creating database '$database'"
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" -d "$POSTGRES_DB" <<-EOSQL
        SELECT 'CREATE DATABASE $database'
        WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '$database')\gexec
        GRANT ALL PRIVILEGES ON DATABASE $database TO $POSTGRES_USER;
EOSQL
}

if [ -n "$POSTGRES_MULTIPLE_DATABASES" ]; then
    echo "Multiple database creation requested: $POSTGRES_MULTIPLE_DATABASES"
    for db in $(echo $POSTGRES_MULTIPLE_DATABASES | tr ',' ' '); do
        create_user_and_database $db
        if [ "$db" = "airflow" ] && [ -n "${AIRFLOW_DB_USER:-}" ] && [ -n "${AIRFLOW_DB_PASSWORD:-}" ]; then
            echo "Creating user '$AIRFLOW_DB_USER' for database '$db'"
            psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" -d "$POSTGRES_DB" <<-EOSQL
                do
                \$\$
                begin
                  if not exists (select from pg_catalog.pg_roles where rolname = '$AIRFLOW_DB_USER') then
                    create role $AIRFLOW_DB_USER with encrypted password '$AIRFLOW_DB_PASSWORD';
                    alter role $AIRFLOW_DB_USER with login;
                  end if;
                end
                \$\$;
                GRANT ALL PRIVILEGES ON DATABASE $db TO $AIRFLOW_DB_USER;
                ALTER DATABASE $db OWNER TO $AIRFLOW_DB_USER;
EOSQL
        fi
    done
    echo "Multiple databases created"
fi
