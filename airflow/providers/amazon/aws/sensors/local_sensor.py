from __future__ import annotations

import asyncio
import time
from typing import Any

from airflow.operators.python import get_current_context
from airflow.sensors.base import BaseSensorOperator, PokeReturnValue
from airflow.utils.context import Context


class LocalSensor(BaseSensorOperator):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def poke(self, context: Context) -> bool | PokeReturnValue:
        time.sleep(2)

        current_context = get_current_context(sensor_trigger_context=True)
        self.log.info(current_context)
        self.log.info(current_context["ti"].task_id)
        self.log.info(current_context["ti"].map_index)
        self.log.info(context["ti"].task_id)
        self.log.info(context["ti"].map_index)
        task_instance = context['ti']
        task_instance.xcom_push(key='my_key', value='my_value')
        self.log.info(self.task_id)
        return True

