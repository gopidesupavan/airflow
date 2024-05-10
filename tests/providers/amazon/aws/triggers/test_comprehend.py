from __future__ import annotations

from unittest import mock
from unittest.mock import AsyncMock

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.comprehend import ComprehendHook
from airflow.providers.amazon.aws.triggers.comprehend import ComprehendPiiEntitiesDetectionJobCompletedTrigger
from airflow.triggers.base import TriggerEvent
from tests.providers.amazon.aws.utils.test_waiter import assert_expected_waiter_type

BASE_TRIGGER_CLASSPATH = "airflow.providers.amazon.aws.triggers.comprehend."


class TestBaseComprehendTrigger:
    EXPECTED_WAITER_NAME: str | None = None
    JOB_ID: str | None = None

    def test_setup(self):
        # Ensure that all subclasses have an expected waiter name set.
        if self.__class__.__name__ != "TestBaseComprehendTrigger":
            assert isinstance(self.EXPECTED_WAITER_NAME, str)
            assert isinstance(self.JOB_ID, str)


class TestBedrockProvisionModelThroughputCompletedTrigger(TestBaseComprehendTrigger):
    EXPECTED_WAITER_NAME = "pii_entities_detection_job_complete"
    JOB_ID = "job_id"

    def test_serialization(self):
        """Assert that arguments and classpath are correctly serialized."""
        trigger = ComprehendPiiEntitiesDetectionJobCompletedTrigger(
            job_id=self.JOB_ID
        )
        classpath, kwargs = trigger.serialize()
        print(kwargs)
        assert classpath == BASE_TRIGGER_CLASSPATH + "ComprehendPiiEntitiesDetectionJobCompletedTrigger"
        assert kwargs.get("job_id") == self.JOB_ID

    @pytest.mark.asyncio
    @mock.patch.object(ComprehendHook, "get_waiter")
    @mock.patch.object(ComprehendHook, "async_conn")
    async def test_run_success(self, mock_async_conn, mock_get_waiter):
        mock_async_conn.__aenter__.return_value = mock.MagicMock()
        mock_get_waiter().wait = AsyncMock()
        trigger = ComprehendPiiEntitiesDetectionJobCompletedTrigger(
            job_id=self.JOB_ID
        )

        generator = trigger.run()
        response = await generator.asend(None)

        assert response == TriggerEvent(
            {"status": "success", "job_id": self.JOB_ID}
        )
        assert_expected_waiter_type(mock_get_waiter, self.EXPECTED_WAITER_NAME)
        mock_get_waiter().wait.assert_called_once()
