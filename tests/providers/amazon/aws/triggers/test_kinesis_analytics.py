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
from unittest.mock import AsyncMock, patch, Mock

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.triggers.kinesis_analytics import \
    KinesisAnalyticsV2ApplicationOperationCompleteTrigger
from airflow.triggers.base import TriggerEvent

BASE_TRIGGER_CLASSPATH = "airflow.providers.amazon.aws.triggers.kinesis_analytics."
MOCK_EXCEPTION = AirflowException("Mock Error")


class TestKinesisAnalyticsV2ApplicationOperationCompleteTrigger:
    APPLICATION_NAME = "demo"
    OPERATION_ID = "1234"

    def setup_method(self):
        self.async_conn_patcher = patch(
            "airflow.providers.amazon.aws.hooks.kinesis_analytics.KinesisAnalyticsV2Hook.async_conn")
        self.mock_async_conn = self.async_conn_patcher.start()

        self.mock_client = AsyncMock()
        self.mock_async_conn.__aenter__.return_value = self.mock_client

        self.async_wait_patcher = patch(
            "airflow.providers.amazon.aws.triggers.kinesis_analytics.async_wait", return_value=True
        )
        self.mock_async_wait = self.async_wait_patcher.start()

    def test_serialization_with(self):
        """Assert that arguments and classpath are correctly serialized."""
        trigger = KinesisAnalyticsV2ApplicationOperationCompleteTrigger(
            application_name=self.APPLICATION_NAME,
            operation_id=self.OPERATION_ID,
            waiter_name="application_start_complete"
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == BASE_TRIGGER_CLASSPATH + "KinesisAnalyticsV2ApplicationOperationCompleteTrigger"
        assert kwargs.get("application_name") == self.APPLICATION_NAME
        assert kwargs.get("operation_id") == self.OPERATION_ID

    @pytest.mark.asyncio
    async def test_run_success_for_application_start_complete_waiter(self):
        self.mock_client.get_waiter = Mock(return_value="waiter")
        trigger = KinesisAnalyticsV2ApplicationOperationCompleteTrigger(
            application_name=self.APPLICATION_NAME,
            operation_id=self.OPERATION_ID,
            waiter_name="application_start_complete"
        )

        generator = trigger.run()
        response = await generator.asend(None)

        assert response == TriggerEvent({"status": "success", "application_name": self.APPLICATION_NAME,
                                         "operation_id": self.OPERATION_ID})
        self.mock_client.get_waiter.assert_called_once_with("application_start_complete")

    @pytest.mark.asyncio
    async def test_when_start_application_raises_exception_it_should_return_a_failure_event(self):
        self.mock_async_wait.side_effect = MOCK_EXCEPTION

        trigger = KinesisAnalyticsV2ApplicationOperationCompleteTrigger(
            application_name=self.APPLICATION_NAME,
            operation_id=self.OPERATION_ID,
            waiter_name="application_start_complete"
        )

        generator = trigger.run()
        response = await generator.asend(None)

        assert response == TriggerEvent({"status": "failed", "application_name": self.APPLICATION_NAME,
                                         "operation_id": self.OPERATION_ID})

    @pytest.mark.asyncio
    async def test_run_success_for_application_stop_complete_waiter(self):
        self.mock_client.get_waiter = Mock(return_value="waiter")

        trigger = KinesisAnalyticsV2ApplicationOperationCompleteTrigger(
            application_name=self.APPLICATION_NAME,
            operation_id=self.OPERATION_ID,
            waiter_name="application_stop_complete"
        )

        generator = trigger.run()
        response = await generator.asend(None)

        assert response == TriggerEvent({"status": "success", "application_name": self.APPLICATION_NAME,
                                         "operation_id": self.OPERATION_ID})
        self.mock_client.get_waiter.assert_called_once_with("application_stop_complete")

    @pytest.mark.asyncio
    async def test_when_stop_application_raises_exception_it_should_return_a_failure_event(self):
        self.mock_async_wait.side_effect = MOCK_EXCEPTION

        trigger = KinesisAnalyticsV2ApplicationOperationCompleteTrigger(
            application_name=self.APPLICATION_NAME,
            operation_id=self.OPERATION_ID,
            waiter_name="application_stop_complete"
        )

        generator = trigger.run()
        response = await generator.asend(None)

        assert response == TriggerEvent({"status": "failed", "application_name": self.APPLICATION_NAME,
                                         "operation_id": self.OPERATION_ID})

