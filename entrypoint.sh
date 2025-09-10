#!/usr/bin/env bash

echo "Initializing the Airflow database..."
airflow db init

echo "Creating admin user..."
airflow users create \
    --username "${AIRFLOW_ADMIN_USERNAME:-admin}" \
    --firstname "${AIRFLOW_ADMIN_FIRSTNAME:-Petyr}" \
    --lastname "${AIRFLOW_ADMIN_LASTNAME:-Baelish}" \
    --role Admin \
    --email "${AIRFLOW_ADMIN_EMAIL:-little_finger@example.com}" \
    --password "${AIRFLOW_ADMIN_PASSWORD:-admin}"

echo "Starting $1..."
exec airflow "$1"