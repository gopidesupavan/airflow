from typing import AsyncIterator, Any

from sqlalchemy.util import asyncio

from airflow.triggers.base import BaseTrigger, TriggerEvent


class CustomTrigger(BaseTrigger):

    def __init__(self,sqs_queue, **kwargs):
        self.sqs_queue = sqs_queue

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            self.__class__.__module__ + "." + self.__class__.__qualname__,
            {
                "sqs_queue": "dummy_queue",
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        print("Running trigger")
        await asyncio.sleep(20)
        yield TriggerEvent({"status": "success", "message_batch": "dummy_message_batch"})
