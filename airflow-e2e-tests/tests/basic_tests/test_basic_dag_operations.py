from clients import AirflowClient

from clients import TaskSDKClient



class TestBasicDagFunctionality:
    airflow_client = AirflowClient()

    def test_dag_unpause(self):
        resp = self.airflow_client.un_pause_dag(
           "example_short_circuit_operator_debug",
        )
        print(f"Response: {resp}")

class TestTaskSDKBasicFunctionality:
    task_sdk_client = TaskSDKClient()

    def test_task_sdk_health_check(self):
        resp = self.task_sdk_client.health_check()
        print(f"Response: {resp}")
