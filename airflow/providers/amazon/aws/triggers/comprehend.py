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

if TYPE_CHECKING:
    from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook

from airflow.providers.amazon.aws.triggers.base import AwsBaseWaiterTrigger
from airflow.providers.amazon.aws.hooks.comprehend import ComprehendHook


class ComprehendPiiEntitiesDetectionJobCompletedTrigger(AwsBaseWaiterTrigger):
    def __init__(
        self, *, job_id: str, waiter_delay: int, waiter_max_attempts: int, aws_conn_id: str | None = None
    ) -> None:
        super().__init__(
            serialized_fields={"job_id": job_id},
            waiter_name="pii_entities_detection_job_complete",
            waiter_args={"JobId": job_id},
            failure_message="Comprehend start pii entities detection job failed.",
            status_message="Status of Comprehend start pii entities detection job is",
            status_queries=["PiiEntitiesDetectionJobProperties.JobStatus"],
            return_key="job_id",
            return_value=job_id,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
        )

    def hook(self) -> AwsGenericHook:
        return ComprehendHook(aws_conn_id=self.aws_conn_id)
