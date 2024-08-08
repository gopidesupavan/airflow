from __future__ import annotations

import asyncio

from airflow.operators.python import get_current_context
from airflow.sensors.base import BaseSensorOperator, PokeReturnValue, InternalSensorTrigger
from airflow.triggers.base import StartTriggerArgs
from airflow.utils.context import Context


class TestPerfSensor(BaseSensorOperator):
    start_trigger_args = StartTriggerArgs(
        trigger_cls="airflow.sensors.base.InternalSensorTrigger",
        trigger_kwargs={},
        next_method="execute_complete",
        next_kwargs=None,
        timeout=None,
    )

    def __init__(self, param: int,
                 start_from_trigger: bool = False,
                 **kwargs):
        super().__init__(**kwargs)
        self.param = param
        self.start_from_trigger = start_from_trigger
        self.start_trigger_args.trigger_kwargs = {}

    async def validation_check(self):
        await asyncio.sleep(120)
        self.log.info("Validate check completed")

    async def poke(self, context: Context) -> bool | PokeReturnValue:
        current_context = get_current_context(sensor_trigger_context=True)
        self.log.info(current_context)
        self.log.info(current_context["ti"].task_id)
        self.log.info(current_context["ti"].map_index)
        task_instance = context['ti']
        task_instance.xcom_push(key='my_key', value=current_context["ti"].map_index)
        await self.validation_check()
        return True

    def execute(self, context: Context) -> None:
        self.defer(
            trigger=InternalSensorTrigger(),
            method_name="execute_complete",
        )
