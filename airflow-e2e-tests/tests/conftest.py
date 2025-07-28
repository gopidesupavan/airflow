# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import annotations
from testcontainers.compose import DockerCompose
import os

import pytest
import json
from datetime import datetime
from constants import DOCKER_COMPOSE_HOST_PORT, DOCKER_COMPOSE_PATH, DOCKER_IMAGE

compose_instance = None

def spin_up_airflow_environment():
    global compose_instance
    os.environ["AIRFLOW_IMAGE_NAME"] = DOCKER_IMAGE
    compose_instance = DockerCompose(DOCKER_COMPOSE_PATH,
                       compose_file_name=["docker-compose.yaml"],
                       pull=False)
    compose_instance.start()
    compose_instance.wait_for(f"http://{DOCKER_COMPOSE_HOST_PORT}/api/v2/version")




def pytest_sessionstart(session):
    print("Spinning airflow environment...")

    spin_up_airflow_environment()

    print("Airflow environment is up and running!")

test_results = []


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    """Hook to capture test results"""
    outcome = yield
    report = outcome.get_result()

    if report.when == "call":
        test_result = {
            "test_name": item.name,
            "test_class": item.cls.__name__ if item.cls else "No Class",
            "status": report.outcome,
            "duration": report.duration,
            "error": str(report.longrepr) if report.failed else None,
            "timestamp": datetime.now().isoformat()
        }
        test_results.append(test_result)


def pytest_sessionfinish(session, exitstatus):
    """Generate report after all tests complete"""
    generate_test_report(test_results)
    if compose_instance:
        compose_instance.stop()


def generate_test_report(results):
    """Generate comprehensive test report"""
    report = {
        "summary": {
            "total_tests": len(results),
            "passed": len([r for r in results if r["status"] == "passed"]),
            "failed": len([r for r in results if r["status"] == "failed"]),
            "execution_time": sum(r["duration"] for r in results)
        },
        "test_results": results
    }

    # Save JSON report
    with open("test_report.json", "w") as f:
        json.dump(report, f, indent=2)

    # Generate HTML report (simplified)

    print(f"\n{'=' * 50}")
    print("TEST EXECUTION SUMMARY")
    print(f"{'=' * 50}")
    print(f"Total Tests: {report['summary']['total_tests']}")
    print(f"Passed: {report['summary']['passed']}")
    print(f"Failed: {report['summary']['failed']}")
    print(f"Execution Time: {report['summary']['execution_time']:.2f}s")
    print(f"Reports generated: test_report.json, test_report.html")
