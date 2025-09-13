FROM apache/airflow:2.5.1

# Copy DAGs into the container
COPY airflow/dags/ /opt/airflow/dags/

# Install dependencies
RUN pip install apache-airflow-providers-mysql pandas numpy mysqlclient