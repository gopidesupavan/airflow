from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import HttpOperator

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

    https_operator = HttpOperator(
        task_id="https_operator",
        http_conn_id="https_host-has-no-schema",
        endpoint="products",
        method="GET",
    )

    https_sensor = HttpSensor(
        task_id="https_sensor",
        http_conn_id="https_host-has-no-schema",
        endpoint="fake_endpoint",
        method="GET",
        deferrable=True,
        poke_interval=30,
        timeout=60,
    )

    https_operator >> https_sensor