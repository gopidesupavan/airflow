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

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from airflow.providers.common.ai.utils.datasource import StorageType


@dataclass(frozen=True)
class ConnectionConfig:
    """Configuration for datafusion object store connections"""

    conn_id: str
    credentials: dict[str, Any] = field(default_factory=dict)
    extra_config: dict[str, Any] = field(default_factory=dict)


class FormatType(str, Enum):
    """Supported data formats."""

    PARQUET = "parquet"
    CSV = "csv"
    JSON = "json"
    AVRO = "avro"


class QueryResultOutputFormat(str, Enum):
    TABULATE= "tabulate"
    JSON = "json"
