import typing

from airflow.sdk.execution_time.context import get_dag_run_count_by_state
from airflow.triggers.base import TriggerEvent, BaseTrigger


class SampleTrigger(BaseTrigger):
    """
    A sample trigger to demonstrate how to create a trigger.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def serialize(self) -> tuple[str, dict[str, typing.Any]]:
        """Serialize the trigger param and module path."""
        return "airflow.providers.standard.triggers.sample_trigger.SampleTrigger", {}

    async def run(self) -> typing.AsyncIterator[TriggerEvent]:

        resp = get_dag_run_count_by_state(
            dag_id="check_conn",
            states=["success"],
            runids=["manual__2025-03-26T22:21:02.313310+00:00_Z3Kkz4mK", "manual__2025-03-26T22:21:20.235092+00:00_MeXGeJoG"]
        )
        print(resp)

        yield TriggerEvent({"status": "success", "response": resp})

