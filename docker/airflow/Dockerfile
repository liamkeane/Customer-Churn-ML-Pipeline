FROM apache/airflow:2.5.1

# Copy DAGs into the container
COPY airflow/dags/ /opt/airflow/dags/

# Install dependencies
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r docker/airflow/requirements.txt