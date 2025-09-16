FROM jupyter/datascience-notebook:latest

# Set environment variables
ENV JUPYTER_ENABLE_LAB=yes

# Install additional system dependencies
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        vim \
        git \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

# Switch back to the default user
USER $NB_UID

# Install additional Python packages
RUN pip install --no-cache-dir \
    mysql-connector-python \
    sqlalchemy \
    pandas-profiling \
    seaborn \
    scikit-learn \
    pyarrow \
    pymysql