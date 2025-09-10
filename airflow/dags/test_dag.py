from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    "example_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    task = BashOperator(
        task_id="hello_world",
        bash_command="echo 'Hello, Airflow!'"
    )
