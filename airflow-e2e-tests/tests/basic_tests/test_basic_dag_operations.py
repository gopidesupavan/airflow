from clients import AirflowClient

from clients import TaskSDKClient

airflow_client = AirflowClient()
task_sdk_client = TaskSDKClient()

class TestBasicDagFunctionality:

    def test_dag_unpause(self):
        resp = airflow_client.un_pause_dag(
           "example_short_circuit_operator_debug",
        )
        print(f"Response: {resp}")

class TestTaskSDKBasicFunctionality:

    def test_task_sdk_health_check(self):
        resp = task_sdk_client.health_check()
        print(f"Response: {resp}")
