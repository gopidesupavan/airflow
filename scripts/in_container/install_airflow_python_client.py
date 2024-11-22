#!/usr/bin/env python3
import sys
from pathlib import Path

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

from in_container_utils import click, console, run_command

AIRFLOW_SOURCE_DIR = Path(__file__).resolve().parents[1]
DIST_FOLDER = Path("/dist")


def find_airflow_python_client(extension: str):
    packages = [f.as_posix() for f in DIST_FOLDER.glob(f"apache_airflow_client-[0-9]*.{extension}")]
    if len(packages) > 1:
        console.print(f"\n[red]Found multiple airflow client packages: {packages}\n")
        sys.exit(1)
    elif len(packages) == 0:
        console.print("\n[red]No airflow client package found\n")
        sys.exit(1)
    if packages:
        console.print(f"\n[bright_blue]Found airflow client package: {packages[0]}\n")
    else:
        console.print("\n[yellow]No airflow client package found.\n")
    return packages[0]

@click.command()
@click.option(
    "--github-actions",
    is_flag=True,
    default=False,
    show_default=True,
    envvar="GITHUB_ACTIONS",
    help="Running in GitHub Actions",
)
def install_airflow_python_client(github_actions: bool):
    base_install_airflow_cmd = [
        "/usr/local/bin/uv",
        "pip",
        "install",
        "--python",
        "/usr/local/bin/python",
        find_airflow_python_client("whl")
    ]
    console.print("\n[bright_blue]Installing airflow python client\n")
    run_command(base_install_airflow_cmd, github_actions=github_actions, check=True)



if __name__ == "__main__":
    install_airflow_python_client()
