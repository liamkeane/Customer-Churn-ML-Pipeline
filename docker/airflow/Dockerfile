FROM apache/airflow:2.5.0

USER root

# Install any necessary packages
RUN apt-get update && apt-get install -y \
    python3-pip \
    && pip3 install --no-cache-dir \
    apache-airflow-providers-cncf-kubernetes \
    apache-airflow-providers-docker \
    apache-airflow-providers-sqlite

USER airflow
