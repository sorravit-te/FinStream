#!/usr/bin/env bash

set -Eeuo pipefail

required_variables=(
  POSTGRES_USER
  POSTGRES_DB
  FINSTREAM_POSTGRES_DB
  FINSTREAM_POSTGRES_USER
  FINSTREAM_POSTGRES_PASSWORD
  AIRFLOW_POSTGRES_DB
  AIRFLOW_POSTGRES_USER
  AIRFLOW_POSTGRES_PASSWORD
)

for variable_name in "${required_variables[@]}"; do
  if [[ -z "${!variable_name:-}" ]]; then
    printf '%s must be set for PostgreSQL initialization\n' "$variable_name" >&2
    exit 1
  fi
done

if [[ "$FINSTREAM_POSTGRES_USER" == "$POSTGRES_USER" || "$AIRFLOW_POSTGRES_USER" == "$POSTGRES_USER" || "$FINSTREAM_POSTGRES_USER" == "$AIRFLOW_POSTGRES_USER" ]]; then
  printf '%s\n' 'PostgreSQL bootstrap, FinStream, and Airflow roles must be distinct' >&2
  exit 1
fi

if [[ "$FINSTREAM_POSTGRES_DB" == "$POSTGRES_DB" || "$AIRFLOW_POSTGRES_DB" == "$POSTGRES_DB" || "$FINSTREAM_POSTGRES_DB" == "$AIRFLOW_POSTGRES_DB" ]]; then
  printf '%s\n' 'PostgreSQL bootstrap, FinStream, and Airflow databases must be distinct' >&2
  exit 1
fi

psql \
  --set=ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  --set=finstream_database="$FINSTREAM_POSTGRES_DB" \
  --set=finstream_user="$FINSTREAM_POSTGRES_USER" \
  --set=finstream_password="$FINSTREAM_POSTGRES_PASSWORD" \
  --set=airflow_database="$AIRFLOW_POSTGRES_DB" \
  --set=airflow_user="$AIRFLOW_POSTGRES_USER" \
  --set=airflow_password="$AIRFLOW_POSTGRES_PASSWORD" <<'SQL'
SELECT format(
    'CREATE ROLE %I LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT PASSWORD %L',
    :'finstream_user',
    :'finstream_password'
)
WHERE NOT EXISTS (
    SELECT 1 FROM pg_roles WHERE rolname = :'finstream_user'
)
\gexec

SELECT format(
    'ALTER ROLE %I LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT PASSWORD %L',
    :'finstream_user',
    :'finstream_password'
)
\gexec

SELECT format(
    'CREATE ROLE %I LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT PASSWORD %L',
    :'airflow_user',
    :'airflow_password'
)
WHERE NOT EXISTS (
    SELECT 1 FROM pg_roles WHERE rolname = :'airflow_user'
)
\gexec

SELECT format(
    'ALTER ROLE %I LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT PASSWORD %L',
    :'airflow_user',
    :'airflow_password'
)
\gexec

SELECT format(
    'CREATE DATABASE %I OWNER %I',
    :'finstream_database',
    :'finstream_user'
)
WHERE NOT EXISTS (
    SELECT 1 FROM pg_database WHERE datname = :'finstream_database'
)
\gexec

SELECT format(
    'ALTER DATABASE %I OWNER TO %I',
    :'finstream_database',
    :'finstream_user'
)
\gexec

SELECT format(
    'CREATE DATABASE %I OWNER %I',
    :'airflow_database',
    :'airflow_user'
)
WHERE NOT EXISTS (
    SELECT 1 FROM pg_database WHERE datname = :'airflow_database'
)
\gexec

SELECT format(
    'ALTER DATABASE %I OWNER TO %I',
    :'airflow_database',
    :'airflow_user'
)
\gexec
SQL
