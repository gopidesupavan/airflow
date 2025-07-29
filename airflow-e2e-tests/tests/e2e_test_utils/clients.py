import time
from functools import cached_property

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from constants import DOCKER_COMPOSE_HOST_PORT, AIRFLOW_WWW_USER_USERNAME, AIRFLOW_WWW_USER_PASSWORD

class AirflowClient:
    def __init__(self):
        self.session = requests.Session()

    @cached_property
    def token(self):
        Retry.DEFAULT_BACKOFF_MAX = 32
        retry = Retry(total=10, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        session = requests.Session()
        session.mount("http://", HTTPAdapter(max_retries=retry))
        session.mount("https://", HTTPAdapter(max_retries=retry))

        api_server_url = DOCKER_COMPOSE_HOST_PORT
        if not api_server_url.startswith(("http://", "https://")):
            api_server_url = "http://" + DOCKER_COMPOSE_HOST_PORT

        url = f"{api_server_url}/auth/token"

        login_response = session.post(
            url,
            json={"username": AIRFLOW_WWW_USER_USERNAME, "password": AIRFLOW_WWW_USER_PASSWORD},
        )
        access_token = login_response.json().get("access_token")

        assert access_token, f"Failed to get JWT token from redirect url {url} with status code {login_response}"
        return access_token

    def _make_request(self, method: str, endpoint: str,  base_url: str = f"http://{DOCKER_COMPOSE_HOST_PORT}/api/v2", **kwargs):
        response = requests.request(
            method=method,
            url=f"{base_url}/{endpoint}",
            headers={"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"},
            **kwargs,
        )
        response.raise_for_status()
        return response.json()

    def get_dags(self):
        pass

    def un_pause_dag(self, dag_id: str):
       return self._make_request(
           method="PATCH",
           endpoint=f"dags/{dag_id}",
           json={"is_paused": False},
       )

    def trigger_dag(self, dag_id: str, json=None):
        if json is None:
            json = {}
        return self._make_request(
            method="POST",
            endpoint=f"dags/{dag_id}/dagRuns",
            json=json
        )

    def wait_for_dag_run(self, dag_id: str, run_id: str, timeout=180, check_interval=5):
        start_time = time.time()
        while time.time() - start_time < timeout:
            response = self._make_request(
                method="GET",
                endpoint=f"dags/{dag_id}/dagRuns/{run_id}",
            )
            print(response)
            state = response.get("state")
            if state in {"success", "failed"}:
                return state
            time.sleep(check_interval)
        raise TimeoutError(f"DAG run {run_id} for DAG {dag_id} did not complete within {timeout} seconds.")

    def get_xcom_value(self, dag_id: str, task_id: str, run_id: str, key:str, map_index=-1):
        return self._make_request(
            method="GET",
            endpoint=f"dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/xcomEntries/{key}?map_index={map_index}"
        )


class TaskSDKClient:
    def __init__(self):
        pass

    @cached_property
    def client(self):
        from airflow.sdk.api.client import Client
        client = Client(base_url=f"http://{DOCKER_COMPOSE_HOST_PORT}/execution", token="not-a-token")
        return client

    def health_check(self):
        response = self.client.get("health/ping", headers={"Airflow-API-Version": "2025-08-10"})
        return response

