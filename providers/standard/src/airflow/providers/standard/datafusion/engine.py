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

import logging

from datafusion import SessionContext

from airflow.providers.standard.datafusion.config import ConnectionConfig, DataSourceConfig, StorageType
from airflow.providers.standard.datafusion.connection_manager import ConnectionManager
from airflow.providers.standard.datafusion.format_handlers import FormatHandlerFactory
from airflow.providers.standard.datafusion.object_store_provider import ObjectStorageProviderFactory
from airflow.utils.log.logging_mixin import LoggingMixin

logger = logging.getLogger(__name__)


class DataFusionEngine(LoggingMixin):
    """DataFusionEngine manages the DataFusion context, data sources, and query execution."""

    def __init__(self):
        super().__init__()
        self.ctx = SessionContext()
        self.format_handler_factory = FormatHandlerFactory()
        self.storage_factory = ObjectStorageProviderFactory()
        self.connection_manager = ConnectionManager()

        self.registered_tables: dict[str, str] = {}

    def register_data_source(self, config: DataSourceConfig):
        """
        Register a data source with the DataFusion context.

        Register format handlers.
        """
        connection_config = self.connection_manager.get_connection_config(config.connection_id)

        if self._require_object_store(connection_config.storage_type):
            self._register_object_store(connection_config, path=config.path)

        format_handler = self.format_handler_factory.get_handler(config.format_type.value)
        format_handler.register_data_source_format(self.ctx, config)

        self.registered_tables[config.table_name] = config.path

    @staticmethod
    def _require_object_store(storage_type: StorageType) -> bool:
        """
        Determine if an object store is required based on storage type.

        Table providers are handled in format handlers, so return False for them.
        """
        if storage_type:
            return storage_type in StorageType
        return False

    def _register_object_store(self, conn_config: ConnectionConfig, path: str | None = None):
        """Register an object store with the DataFusion context."""
        if not conn_config.storage_type:
            raise ValueError(
                "Storage type must be specified in connection config for object store registration."
            )

        storage_provider = self.storage_factory.create_provider(conn_config.storage_type.value)
        object_store = storage_provider.create_object_store(conn_config, path=path)

        schema = storage_provider.get_scheme()
        self.ctx.register_object_store(schema=schema, store=object_store)

        self.log.info("Registered object store for schema: %s", schema)

    def execute_query(self, query: str):
        try:
            df = self.ctx.sql(query)
            return df.to_pydict()
        except Exception as e:
            self.log.error("Failed to execute query: %s", e)
            raise
