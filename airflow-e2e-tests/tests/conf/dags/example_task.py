from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import HttpOperator
from airflow.sdk import task
from pendulum import DateTime

with DAG(
    dag_id="dag_reproduce_issue",
    description="Reproduce",
    start_date=datetime.now() - timedelta(days=1),
    schedule=None,
    catchup=False,
    default_args={
        "retries": 0,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["reproduce_issue"],
) as dag:

    @task
    def print_context(logical_date: DateTime):
        print(f"Logical date: {logical_date}")


    print_context()