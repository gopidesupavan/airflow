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

import os
from typing import Any, TYPE_CHECKING

from airflow.providers.standard.datafusion.base import TableProvider
from airflow.providers.standard.datafusion.config import ConnectionConfig, DataSourceConfig, TableProviderType

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog

class IcebergTableProvider(TableProvider):
    """Iceberg Table Provider using PyIceberg."""

    def get_provider_type(self) -> str:
        """Return the table provider type."""
        return TableProviderType.ICEBERG.value

    def get_table(self, config: DataSourceConfig, connection_config: ConnectionConfig) -> Any:
        """Create and return Iceberg table instance using PyIceberg."""
        try:
            # Temporary environment variable, for now aws support, add others later
            os.environ["AWS_ACCESS_KEY_ID"] = connection_config.extra_config.get("client.access-key-id", "")
            os.environ["AWS_SECRET_ACCESS_KEY"] = connection_config.extra_config.get(
                "client.secret-access-key", ""
            )
            os.environ["AWS_REGION"] = connection_config.extra_config.get("client.region", "")

            catalog = self._load_catalog(connection_config.extra_config)

            self.log.info(
                "Loaded Iceberg catalog for table %s in database %s", config.table_name, config.db_name
            )

            if config.table_name is None or config.db_name is None:
                raise ValueError("Database name and table name must be provided for Iceberg tables")

            return catalog.load_table((config.db_name, config.table_name))
        except Exception as e:
            raise RuntimeError(f"Failed to create Iceberg table provider: {e}") from e

    @staticmethod
    def _load_catalog(catalog_properties: dict[str, str]) -> Catalog:
        """Load Iceberg catalog using PyIceberg."""
        from pyiceberg.catalog import load_catalog

        return load_catalog(**catalog_properties)
