# from __future__ import annotations
#
# from datetime import datetime
#
# from airflow import DAG
# from airflow.operators.empty import EmptyOperator
# from airflow.sensors.dummy_sensor import DummySensor
#
# DAG_ID = "dummy_sensor"
#
# with DAG(
#     dag_id=DAG_ID,
#     schedule=None,
#     start_date=datetime(2021, 1, 1),
#     tags=["example"],
#     catchup=False,
# ) as dag:
#     EmptyOperator(task_id="test") >> DummySensor(task_id="dummy_sensor", local_id="1234567890")
