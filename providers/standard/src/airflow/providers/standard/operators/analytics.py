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

from airflow.providers.standard.datafusion.config import DataSourceConfig
from airflow.providers.standard.datafusion.engine import DataFusionEngine
from airflow.sdk import BaseOperator


class AnalyticsOperator(BaseOperator):
    """An operator that performs a simple analytics task."""

    template_fields = ("data_source_configs",)

    def __init__(self, data_source_configs: list[dict] | str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.data_source_configs = data_source_configs

    def execute(self, context):
        self.engine = DataFusionEngine()

        if isinstance(self.data_source_configs, str):
            import json

            self.data_source_configs = json.loads(self.data_source_configs)

        for config in self.data_source_configs:
            self.engine.register_data_source(DataSourceConfig.model_validate(config))

        return self._execute_queries()

    def _execute_queries(self):
        from tabulate import tabulate

        results_tables_str = ""

        for config in map(DataSourceConfig.model_validate, self.data_source_configs):
            for query, prompt in zip(config.sql_queries, config.prompts):

                self.log.info("Executing query : %s", query)
                result_dict = self.engine.execute_query(query)

                num_rows = len(next(iter(result_dict.values())))
                rows = [{key: result_dict[key][i] for key in result_dict.keys()} for i in range(num_rows)]

                table_str = tabulate(
                    rows,
                    headers="keys",
                    tablefmt="github",
                    showindex=True,
                )
                results_tables_str += f"\n### Results for Prompt: {prompt}\n\n{table_str}\n\n{'-' * 40}\n"

        return results_tables_str
