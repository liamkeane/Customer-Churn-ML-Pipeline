#!/usr/bin/env bash

echo "Initializing the Airflow database..."
airflow db init

echo "Creating admin user..."
airflow users create \
    --username admin \
    --firstname bob \
    --lastname sneid \
    --role Admin \
    --email admin@example.com \
    --password admin

echo "Starting $1..."
exec airflow "$1"