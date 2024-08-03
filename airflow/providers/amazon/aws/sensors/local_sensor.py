from __future__ import annotations

from typing import Any

from airflow.sensors.base import BaseSensorOperator, PokeReturnValue
from airflow.utils.context import Context


class LocalSensor(BaseSensorOperator):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def poke(self, context: Context) -> bool | PokeReturnValue:

        self.log.info("Inside local sensor")

        return True

    def execute(self, context: Context) -> Any:
        super().execute(context)
