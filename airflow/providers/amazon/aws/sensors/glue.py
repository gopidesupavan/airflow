#
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

from functools import cached_property
from typing import TYPE_CHECKING, Sequence

from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.providers.amazon.aws.hooks.glue import GlueJobHook
from airflow.providers.amazon.aws.hooks.glue_data_quality import GlueDataQualityHook
from airflow.providers.amazon.aws.sensors.base_aws import AwsBaseSensor
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class GlueJobSensor(BaseSensorOperator):
    """
    Waits for an AWS Glue Job to reach any of the status below.

    'FAILED', 'STOPPED', 'SUCCEEDED'

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:GlueJobSensor`

    :param job_name: The AWS Glue Job unique name
    :param run_id: The AWS Glue current running job identifier
    :param verbose: If True, more Glue Job Run logs show in the Airflow Task Logs.  (default: False)
    """

    template_fields: Sequence[str] = ("job_name", "run_id")

    def __init__(
        self,
        *,
        job_name: str,
        run_id: str,
        verbose: bool = False,
        aws_conn_id: str | None = "aws_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.job_name = job_name
        self.run_id = run_id
        self.verbose = verbose
        self.aws_conn_id = aws_conn_id
        self.success_states: list[str] = ["SUCCEEDED"]
        self.errored_states: list[str] = ["FAILED", "STOPPED", "TIMEOUT"]
        self.next_log_tokens = GlueJobHook.LogContinuationTokens()

    @cached_property
    def hook(self):
        return GlueJobHook(aws_conn_id=self.aws_conn_id)

    def poke(self, context: Context):
        self.log.info("Poking for job run status :for Glue Job %s and ID %s", self.job_name, self.run_id)
        job_state = self.hook.get_job_state(job_name=self.job_name, run_id=self.run_id)

        try:
            if job_state in self.success_states:
                self.log.info("Exiting Job %s Run State: %s", self.run_id, job_state)
                return True
            elif job_state in self.errored_states:
                job_error_message = "Exiting Job %s Run State: %s", self.run_id, job_state
                self.log.info(job_error_message)
                # TODO: remove this if block when min_airflow_version is set to higher than 2.7.1
                if self.soft_fail:
                    raise AirflowSkipException(job_error_message)
                raise AirflowException(job_error_message)
            else:
                return False
        finally:
            if self.verbose:
                self.hook.print_job_logs(
                    job_name=self.job_name,
                    run_id=self.run_id,
                    continuation_tokens=self.next_log_tokens,
                )


class GlueDataQualityEvaluationRunSensor(AwsBaseSensor[GlueDataQualityHook]):
    """
    Waits for an AWS Glue data quality evaluation run to reach any of the status below.

    'FAILED', 'STOPPED', 'SUCCEEDED'

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:GlueDataQualityEvaluationRunSensor`

    :param run_id: The AWS Glue data quality evaluation run identifier

    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """

    aws_hook_class = GlueDataQualityHook
    template_fields: Sequence[str] = aws_template_fields("run_id")

    def __init__(
        self,
        *,
        run_id: str,
        aws_conn_id: str | None = "aws_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.run_id = run_id
        self.aws_conn_id = aws_conn_id
        self.success_states: list[str] = ["SUCCEEDED"]
        self.errored_states: list[str] = ["FAILED", "STOPPED", "TIMEOUT"]

    def poke(self, context: Context):
        self.log.info("Poking for AWS Glue data quality run RunId: %s", self.run_id)

        response = self.hook.conn.get_data_quality_ruleset_evaluation_run(RunId=self.run_id)

        if response.get("State") in self.success_states:
            results = self.hook.conn.batch_get_data_quality_result(ResultIds=response["ResultIds"])
            self.log.info(
                "Exiting AWS Glue data quality run RunId: %s Run State: %s", self.run_id, response["State"]
            )
            self.hook.validate_evaluation_results(results)
            return True
        elif response.get("State") in self.errored_states:
            job_error_message = (
                f"Exiting AWS Glue data quality run RunId: {self.run_id} Run State: {response['State']} "
                f"with: {response['ErrorString']}"
            )
            self.log.info(job_error_message)
            # TODO: remove this if block when min_airflow_version is set to higher than 2.7.1
            if self.soft_fail:
                raise AirflowSkipException(job_error_message)
            raise AirflowException(job_error_message)
        else:
            return False
