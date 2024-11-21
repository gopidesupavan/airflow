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
    airflow_package = packages[0] if packages else None
    if airflow_package:
        console.print(f"\n[bright_blue]Found airflow client package: {airflow_package}\n")
    else:
        console.print("\n[yellow]No airflow client package found.\n")
    return airflow_package


def install_airflow_python_client():
    pass



if __name__ == "__main__":
    install_airflow_python_client()
