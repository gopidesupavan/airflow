from datetime import datetime, timezone

from e2e_test_utils.clients import AirflowClient

from e2e_test_utils.clients import TaskSDKClient



class TestBasicDagFunctionality:
    airflow_client = AirflowClient()

    def test_dag_unpause(self):
        self.airflow_client.un_pause_dag(
           "example_xcom_test",
        )

    def test_xcom_value(self):
        resp = self.airflow_client.trigger_dag(
            "example_xcom_test",
            json={"logical_date": datetime.now(timezone.utc).isoformat()}
        )
        self.airflow_client.wait_for_dag_run(
            dag_id="example_xcom_test",
            run_id=resp["dag_run_id"],
        )
        xcom_value_resp = self.airflow_client.get_xcom_value(
            dag_id="example_xcom_test",
            task_id="bash_push",
            key="manually_pushed_value",
            run_id=resp["dag_run_id"],
        )
        assert "manually_pushed_value" == xcom_value_resp["value"], xcom_value_resp


class TestTaskSDKBasicFunctionality:
    task_sdk_client = TaskSDKClient()

    def test_task_sdk_health_check(self):
        resp = self.task_sdk_client.health_check()
        print(f"Response: {resp}")
