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

from datafusion.object_store import AmazonS3, GoogleCloud, LocalFileSystem

from airflow.providers.standard.datafusion.base import ObjectStorageProvider
from airflow.providers.standard.datafusion.config import ConnectionConfig, StorageType


class S3ObjectStorageProvider(ObjectStorageProvider):
    """S3 Object Storage Provider using DataFusion's AmazonS3."""

    def get_storage_type(self) -> str:
        """Return the storage type."""
        return StorageType.S3.value

    def create_object_store(self, connection_config: ConnectionConfig, path: str):
        """Create S3 object store using DataFusion's AmazonS3."""
        try:
            credentials = connection_config.credentials
            bucket = self.get_bucket(path)

            s3_store = AmazonS3(**credentials, bucket_name=bucket)
            self.log.info("Created S3 object store for bucket %s", bucket)

            return s3_store

        except Exception as e:
            self.log.error("Failed to create S3 object store: %s", e)
            raise

    def get_scheme(self) -> str:
        """Return the scheme for S3."""

        return "s3://"


class LocalObjectStorageProvider(ObjectStorageProvider):
    """Local Object Storage Provider using DataFusion's LocalFileSystem."""

    def get_storage_type(self) -> str:
        """Return the storage type."""
        return StorageType.LOCAL.value

    def create_object_store(self, connection_config: ConnectionConfig, path: str):
        """Create Local object store (not implemented)."""
        try:
            prefix = connection_config.extra_config.get("prefix", "/")
            local = LocalFileSystem(prefix=prefix)
            self.log.info("Created Local object store with prefix %s", prefix)
            return local
        except Exception as e:
            self.log.error("Failed to create Local object store: %s", e)
            raise

    def get_scheme(self) -> str:
        """Return the scheme for Local file system."""
        return "file://"


class GCSObjectStorageProvider(ObjectStorageProvider):
    """GCS Object Storage Provider using DataFusion's GoogleCloud."""

    def get_storage_type(self) -> str:
        """Return the storage type."""
        return StorageType.GCS.value

    def create_object_store(self, connection_config: ConnectionConfig, path: str):
        """Create GCS object store using DataFusion's GoogleCloud."""
        bucket = self.get_bucket(path)
        gcs = GoogleCloud(
            bucket_name=bucket, service_account_path=connection_config.credentials.get("service_account_path")
        )

        self.log.info("Creating GCS object store for bucket %s", bucket)
        return gcs

    def get_scheme(self) -> str:
        """Return the scheme for GCS."""
        return "gs://"


class ObjectStorageProviderFactory:
    """Factory to create object storage providers based on storage type."""

    _providers: dict[str, type] = {
        StorageType.S3.value: S3ObjectStorageProvider,
        StorageType.LOCAL.value: LocalObjectStorageProvider,
        StorageType.GCS.value: GCSObjectStorageProvider,
    }

    @classmethod
    def create_provider(cls, storage_type: str) -> ObjectStorageProvider:
        """Create a storage provider instance."""
        if storage_type not in cls._providers:
            raise ValueError(
                f"Unsupported storage type: {storage_type}. Supported types: {list(cls._providers.keys())}"
            )

        provider_class = cls._providers[storage_type]
        return provider_class()
