from __future__ import annotations

from typing import Any

from airflow.operators.python import get_current_context
from airflow.sensors.base import BaseSensorOperator, PokeReturnValue
from airflow.utils.context import Context


class DummySensor(BaseSensorOperator):

    def __init__(self,
                 **kwargs):
        super().__init__(**kwargs)

    def poke(self, context: Context) -> bool | PokeReturnValue:
        contextv = get_current_context()
        self.log.info("It is inside poke method")
        self.log.info(contextv)
        return False

    def execute(self, context: Context) -> Any:
        super().execute(context=context)
