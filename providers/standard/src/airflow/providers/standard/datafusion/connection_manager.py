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
import shutil
import tempfile
from typing import Any

from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.providers.standard.datafusion.config import ConnectionConfig, StorageType, TableProviderType
from airflow.sdk import BaseHook, Connection
from airflow.utils.log.logging_mixin import LoggingMixin


class ConnectionManager(LoggingMixin):
    """Manages connections to Data Fusion instances."""

    def __init__(self):
        super().__init__()

    def get_connection_config(self, connection_id: str) -> ConnectionConfig:
        """Get connection configuration from Airflow connection."""
        try:
            airflow_conn = BaseHook.get_connection(connection_id)

            config = self._convert_airflow_connection(airflow_conn)

            self.log.info("Loaded connection config for: %s", connection_id)
            return config

        except Exception as e:
            self.log.error("Failed to get connection config for %s: %s", connection_id, e)
            raise

    def _convert_airflow_connection(self, conn: Connection) -> ConnectionConfig:
        """Convert Airflow connection to framework ConnectionConfig."""
        # Determine storage type from connection type
        storage_type = self._determine_storage_type(conn)
        credentials = {}
        if storage_type:
            credentials = self._extract_credentials(conn, storage_type)

        extra_config = conn.extra_dejson if conn.extra else {}

        # For table providers, set storage_type to None
        if isinstance(storage_type, TableProviderType):
            storage_type = None  # type: ignore[assignment]

        return ConnectionConfig(
            connection_id=conn.conn_id,
            storage_type=storage_type,
            credentials=credentials,
            extra_config=extra_config,
        )

    def _determine_storage_type(self, conn: Connection):
        """Determine storage type from Airflow connection."""
        conn_type = conn.conn_type.lower() if conn.conn_type else ""

        # Map Airflow connection types to storage types
        type_mapping = {
            "aws": StorageType.S3,
            "google_cloud_platform": StorageType.GCS,
            "local": StorageType.LOCAL,
            "iceberg": TableProviderType.ICEBERG,
        }
        if conn_type in type_mapping:
            return type_mapping[conn_type]

        return None

    @classmethod
    def filter_none(cls, params: dict[str, Any]) -> dict[str, Any]:
        """Filter out None values from the dictionary."""
        return {k: v for k, v in params.items() if v is not None}

    def _extract_credentials(
        self, conn: Connection, storage_or_table_type: StorageType | TableProviderType | None
    ) -> dict[str, Any]:
        credentials = {}

        match storage_or_table_type:
            case StorageType.S3:
                s3_conn: AwsGenericHook = AwsGenericHook(aws_conn_id=conn.conn_id, client_type="s3")
                creds = s3_conn.get_credentials()
                credentials.update(
                    {
                        "access_key_id": conn.login or creds.access_key,
                        "secret_access_key": conn.password or creds.secret_key,
                        "session_token": creds.token if creds.token else None,
                        "region": conn.extra_dejson.get("region"),
                        "endpoint": conn.extra_dejson.get("endpoint"),
                    }
                )
                credentials = self.filter_none(credentials)
            case StorageType.GCS:
                gcp_conn: GoogleBaseHook = GoogleBaseHook(gcp_conn_id=conn.conn_id)

                @GoogleBaseHook.provide_gcp_credential_file
                def _get_credentials(_):
                    temp_file_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

                    # Copy the temporary file to a permanent location
                    # Not a best practice for production, refactor later
                    permanent_file_path = os.path.join(
                        tempfile.gettempdir(), f"{conn.conn_id}_gcp_credentials.json"
                    )
                    shutil.copy(temp_file_path, permanent_file_path)

                    credentials["service_account_path"] = permanent_file_path

                _get_credentials(gcp_conn)

            case TableProviderType.ICEBERG:
                """will use extra field from iceberg connection"""
                pass

            case _:
                raise ValueError(f"Unknown storage type: {storage_or_table_type}")
        return credentials
