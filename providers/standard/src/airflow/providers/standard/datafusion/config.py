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

from pydantic import BaseModel


class TableProviderType(Enum):
    """Table provider types for Data Fusion."""

    ICEBERG = "iceberg"


class StorageType(str, Enum):
    """Storage types for Data Fusion."""

    GCS = "gcs"
    S3 = "s3"
    AZURE = "azure"
    LOCAL = "local"
    HTTP = "http"


class FormatType(Enum):
    """Supported data formats."""

    PARQUET = "parquet"
    ICEBERG = "iceberg"


@dataclass(frozen=True)
class ConnectionConfig:
    """Configuration for connections to data sources."""

    connection_id: str
    storage_type: StorageType | None = None
    credentials: dict[str, Any] = field(default_factory=dict)
    extra_config: dict[str, Any] = field(default_factory=dict)


class DataSourceConfig(BaseModel):
    """Configuration for individual data sources."""

    connection_id: str
    path: str
    format_type: FormatType
    table_name: str
    options: dict[str, Any] = field(default_factory=dict)
    table_schema: dict[str, str] | None = None
    db_name: str | None = None
    prompts: list[str] | None = None
    sql_queries: list[str] | None = None
