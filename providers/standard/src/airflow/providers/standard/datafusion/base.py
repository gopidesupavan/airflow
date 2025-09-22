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

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from airflow.providers.standard.datafusion.connection_manager import ConnectionManager
from airflow.utils.log.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from datafusion import SessionContext

    from airflow.providers.standard.datafusion.config import ConnectionConfig, DataSourceConfig


class FormatHandler(LoggingMixin, ABC):
    """Abstract base class for data format handlers."""

    @classmethod
    @abstractmethod
    def get_format(cls) -> str:
        raise NotImplementedError

    @abstractmethod
    def register_data_source_format(self, ctx: SessionContext, config: DataSourceConfig) -> None:
        raise NotImplementedError

    @staticmethod
    def _get_connection_config(config: DataSourceConfig):
        connection_manager = ConnectionManager()
        try:
            return connection_manager.get_connection_config(config.connection_id)
        except ValueError:
            raise ValueError(f"Invalid connection ID: {config.connection_id}")


class ObjectStorageProvider(LoggingMixin, ABC):
    """Abstract base class for object storage providers."""

    @abstractmethod
    def get_storage_type(self) -> str:
        """Return storage type handled by this provider (e.g., 's3', 'gcs', 'local')."""
        raise NotImplementedError

    @abstractmethod
    def create_object_store(self, connection_config: ConnectionConfig, path: str) -> Any:
        """Create and return DataFusion object store instance."""
        raise NotImplementedError

    @abstractmethod
    def get_scheme(self) -> str:
        """Return URL scheme for this storage type (e.g., 's3://', 'gs://')."""
        raise NotImplementedError

    def get_bucket(self, path: str) -> str | None:
        """Extract bucket name from the given path."""
        if path and path.startswith(self.get_scheme()):
            path_parts = path[len(self.get_scheme()) :].split("/", 1)
            return path_parts[0]
        return None


class TableProvider(LoggingMixin, ABC):
    """Abstract base class for table providers."""

    @abstractmethod
    def get_provider_type(self) -> str:
        """Return the table provider type identifier."""
        raise NotImplementedError

    @abstractmethod
    def get_table(self, config: DataSourceConfig, connection_config: ConnectionConfig) -> Any:
        """Create and return DataFusion table provider instance."""
        raise NotImplementedError
