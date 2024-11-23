import os
import subprocess
import time

import pytest
import requests


@pytest.fixture(scope="session", autouse=True)
def start_airflow_webserver():
    """Start Airflow webserver before tests and stop after."""
    # print("Starting Airflow webserver...")
    # process = subprocess.Popen(["airflow", "webserver", "--port", "8080"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    url = "http://localhost:8080/"
    yield
    # print("Waiting for Airflow webserver to be ready...")
    # for _ in range(30):  # Check for up to 150 seconds (30 retries * 5 seconds)
    #     try:
    #         response = requests.get(url, timeout=5)
    #         if response.status_code == 200 and '"status": "healthy"' in response.text:
    #             print("Airflow webserver is ready.")
    #             break
    #     except requests.ConnectionError:
    #         pass
    #     print("Webserver not ready yet, retrying in 5 seconds...")
    #     time.sleep(5)
    # else:
    #     raise RuntimeError("Airflow webserver did not become ready in time.")
    #
    # # yield  # Run the tests
    #
    # print("Stopping Airflow webserver...")
    # # process.terminate()
