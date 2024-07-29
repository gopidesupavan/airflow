from __future__ import annotations

import time
from typing import Any

from airflow.exceptions import AirflowSensorTimeout
from airflow.sensors.base import BaseSensorOperator, PokeReturnValue
from airflow.utils.context import Context


class DummySensor(BaseSensorOperator):

    def __init__(self,
                 id,
                 **kwargs):
        super().__init__(**kwargs)
        self.id = id
        self.id1 = "1233"
        self.id2 = "2e2112"

    def poke(self, context: Context) -> bool | PokeReturnValue:

        return False

    def execute(self, context: Context) -> Any:
        super().execute(context=context)
