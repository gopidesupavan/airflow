import os
from functools import cached_property

import boto3
import subprocess

PROVIDER_NAME_FORMAT = "apache-airflow-providers-{}"

NON_VERSIONED_PACKAGES = ["apache-airflow-providers", "docker-stack", "helm-chart", "apache-airflow"]
from airflow_breeze.utils.console import get_console
s3_client = boto3.client('s3')

class S3DocsPublish:

    def __init__(self, source_dir_path: str, destination_location: str, exclude_docs: str, dry_run: bool = False, overwrite: bool = False):
        self.source_dir_path = source_dir_path
        self.destination_location = destination_location
        self.exclude_docs = exclude_docs
        self.dry_run = dry_run
        self.overwrite = overwrite
        self.errors = []
        self.source_dest_mapping = []

    @cached_property
    def get_all_docs(self):
        get_console().print(f"[info]Getting all docs from {self.source_dir_path}\n")
        return os.listdir(self.source_dir_path)

    @cached_property
    def get_all_excluded_docs(self):
        if self.exclude_docs is None:
            return []
        return self.exclude_docs.split(',')

    @cached_property
    def get_all_eligible_docs(self):
        non_eligible_docs = []

        for doc in self.get_all_docs:
            for excluded_doc in self.get_all_excluded_docs:
                if excluded_doc in NON_VERSIONED_PACKAGES:
                    non_eligible_docs.append(doc)
                    continue

                excluded_provider_name = PROVIDER_NAME_FORMAT.format(excluded_doc.replace('.', '-'))
                if doc == excluded_provider_name:
                    non_eligible_docs.append(doc)

        return list(set(self.get_all_docs) - set(non_eligible_docs))

    def is_doc_version_exists(self, s3_bucket_doc_location: str) -> bool:
        parts = s3_bucket_doc_location[5:].split("/", 1)
        bucket = parts[0]
        key = parts[1]
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=key)

        return response['KeyCount'] > 0

    def sync_docs_to_s3(self, source: str, destination: str):
        get_console().print(f"[info]Syncing {source} to {destination}\n")

        if self.dry_run:
            get_console().print("Dry run enabled. Skipping sync.")
            return (0, "")
        else:
        #result = subprocess.run(["aws", "s3", "sync", "--delete", source, destination ], capture_output=True, text=True)
            result = subprocess.run(["aws", "s3", "ls"], capture_output=True,text=True)
            return (result.returncode, result.stderr)

    def publish_docs_to_s3(self):

        for doc in self.get_all_eligible_docs:
            stable_file_path = f"{self.source_dir_path}/{doc}/stable.txt"
            if os.path.exists(stable_file_path):
                with open(stable_file_path, "r") as stable_file:
                    stable_version = stable_file.read()
                    get_console().print(f"[info]Stable version: {stable_version} for {doc}\n")
            else:
                get_console().print(f"[info]Stable version file not found for {doc}\n")
                self.errors.append(f"Stable version file not found for {doc} in {stable_file_path}")
                continue

            if self.is_doc_version_exists(f"{self.destination_location}{doc}/{stable_version}"):
                if self.overwrite:
                    get_console().print(f"[info]Overwriting existing version {stable_version} for {doc}\n")
                else:
                    get_console().print(f"[info]Skipping doc publish for {doc} as version {stable_version} already exists\n")
                    continue

            current_doc_dest_versioned_folder = f"{self.destination_location}{doc}/{stable_version}"
            current_doc_dest_stable_folder = f"{self.destination_location}{doc}/stable"

            self.source_dest_mapping.append((f"{self.source_dir_path}/{doc}", current_doc_dest_versioned_folder))
            self.source_dest_mapping.append((f"{self.source_dir_path}/{doc}", current_doc_dest_stable_folder))

