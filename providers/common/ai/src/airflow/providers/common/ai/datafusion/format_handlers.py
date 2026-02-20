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
from typing import Optional, Any

from datafusion import SessionContext

from airflow.providers.common.ai.datafusion.base import FormatHandler
from airflow.providers.common.ai.exceptions import FileFormatRegistrationException
from airflow.providers.common.ai.utils.config import FormatType


class ParquetFormatHandler(FormatHandler):
    def __init__(self, options: Optional[dict[str, Any]] = None):
        super().__init__()
        self.options = options or {}

    def get_format(self) -> str:
        return FormatType.PARQUET.value

    def register_data_source_format(self, ctx: SessionContext, table_name: str, path: str):
        try:
            ctx.register_parquet(table_name, path, **self.options)
        except Exception as e:
            raise FileFormatRegistrationException("Failed to register Parquet data source: %s", e)


class CsvFormatHandler(FormatHandler):
    def __init__(self, options: Optional[dict[str, Any]] = None):
        self.options = options or {}

    def get_format(self) -> str:
        return FormatType.CSV.value

    def register_data_source_format(self, ctx: SessionContext, table_name: str, path: str):
        try:
            ctx.register_csv(table_name, path, **self.options)
        except Exception as e:
            raise FileFormatRegistrationException("Failed to register csv data source: %s", e)


class AvroFormatHandler(FormatHandler):
    def __init__(self, options: Optional[dict[str, Any]] = None):
        self.options = options or {}

    def get_format(self) -> str:
        return FormatType.AVRO.value

    def register_data_source_format(self, ctx: SessionContext, table_name: str, path: str) -> None:
        try:
            ctx.register_avro(table_name, path, **self.options)
        except Exception as e:
            raise FileFormatRegistrationException("Failed to register Avro data source: %s", e)


def get_format_handler(format_type: str, options: Optional[dict[str, Any]] = None) -> FormatHandler:
    format_type = format_type.lower()

    match format_type:
        case "parquet":
            return ParquetFormatHandler(options)
        case "csv":
            return CsvFormatHandler(options)
        case "avro":
            return AvroFormatHandler(options)
        case _:
            raise ValueError(f"Unsupported format: {format_type}")



