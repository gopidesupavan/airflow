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

from typing import TYPE_CHECKING

from airflow.providers.standard.datafusion.base import FormatHandler
from airflow.providers.standard.datafusion.config import DataSourceConfig, FormatType
from airflow.providers.standard.datafusion.table_provider import IcebergTableProvider

if TYPE_CHECKING:
    from datafusion import SessionContext


class ParquetFormatHandler(FormatHandler):
    """Handler for Parquet data format."""

    @classmethod
    def get_format(cls) -> str:
        return FormatType.PARQUET.value

    def register_data_source_format(self, ctx: SessionContext, config: DataSourceConfig) -> None:
        try:
            ctx.register_parquet(config.table_name, config.path)
        except Exception as e:
            raise Exception("Failed to register Parquet data source: %s", e)


class IcebergFormatHandler(FormatHandler):
    """Handler for Iceberg table provider."""

    @classmethod
    def get_format(cls) -> str:
        """Return the format type."""
        return FormatType.ICEBERG.value

    def register_data_source_format(self, ctx: SessionContext, config: DataSourceConfig) -> None:
        """Register Iceberg table provider with the DataFusion context."""

        try:
            table = IcebergTableProvider().get_table(config, self._get_connection_config(config))
            ctx.register_table_provider(config.table_name, table)

            self.log.info("Registered Iceberg table provider for table: %s", config.table_name)

        except Exception as e:
            raise RuntimeError(f"Failed to register Iceberg data source: {e}") from e


class FormatHandlerFactory:
    """Factory to get format handlers based on format type."""

    def __init__(self):
        self._handlers: dict[str, FormatHandler] = {}
        self._register_default_handlers()

    def _register_default_handlers(self) -> None:
        """Register default format handlers."""
        default_handlers = [ParquetFormatHandler(), IcebergFormatHandler()]

        for handler in default_handlers:
            self._handlers[handler.get_format()] = handler

    def get_handler(self, format_type: str) -> FormatHandler | None:
        """Get format handler for specified format type."""
        if format_type not in self._handlers:
            return None
            # raise ValueError(
            #     f"Unsupported format type: {format_type}. Supported formats: {list(self._handlers.keys())}"
            # )
        return self._handlers[format_type]
