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

import json
from typing import TYPE_CHECKING

from pydantic import BaseModel, Field
from pydantic_ai import Agent
from pydantic_ai.models.openai import OpenAIModel  # type: ignore[attr-defined]
from pydantic_ai.providers.github import GitHubProvider
from tabulate import tabulate

from airflow.providers.standard.datafusion.config import DataSourceConfig
from airflow.sdk import BaseHook, BaseOperator

if TYPE_CHECKING:
    from airflow.providers.standard.datafusion.config import DataSourceConfig


class SQLQueryResponse(BaseModel):
    """Response model for SQL query generation."""

    sql_queries: list[str] = Field(description="Provide the SQL queries based on the user prompt.")
    explanation: list[str] = Field(description="Explanation of the generated SQL queries.")


class LLMQueryGeneratorOperator(BaseOperator):
    """
    An operator that generates a query based on a prompt using a language model (LLM).

    :param llm_model: The language model to use for generating the query.
    :param llm_conn_id: The Airflow connection ID for the LLM provider.
    """

    def __init__(
        self,
        data_sources: list[DataSourceConfig],
        llm_model: str = "openai/gpt-4.1",
        llm_conn_id: str = "llm_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.llm_conn_id = llm_conn_id
        self.llm_model = llm_model
        self.data_sources = data_sources

    def execute(self, context):
        connection = BaseHook.get_connection(self.llm_conn_id)

        github_provider = GitHubProvider(
            api_key=connection.password  # PAT token stored in Airflow connection
        )
        github_provider_model = OpenAIModel(
            model_name=self.llm_model,
            provider=github_provider,
        )

        agent = Agent(github_provider_model, output_type=SQLQueryResponse)

        with_queries_datasource = []
        table_data = []

        for datasource in self.data_sources:
            prompt = self._prepare_prompt(datasource)
            result = agent.run_sync(prompt)

            with_queries_datasource.append(
                datasource.model_copy(
                    update={
                        "sql_queries": result.output.sql_queries,
                        "format_type": datasource.format_type.value,
                    }
                ).model_dump()
            )

            # Prepare table data to present to user in a tabular format for any approval step
            table_data.extend(zip(datasource.prompts, result.output.sql_queries))

        table_str = tabulate(table_data, headers=["Prompt", "Generated SQL Query"], tablefmt="github")
        self.log.info("with_queries_datasource: %s", with_queries_datasource)
        return {"input": json.dumps(with_queries_datasource), "view": table_str}

    @staticmethod
    def _prepare_prompt(datasource: DataSourceConfig) -> str:
        schema_str = (
            ", ".join(f"{col}: {typ}" for col, typ in datasource.table_schema.items())
            if datasource.table_schema
            else ""
        )

        queries_section = "\n".join(f"- {p}" for p in datasource.prompts) if datasource.prompts else ""

        prompt = (
            f"TableName: {datasource.table_name}\n"
            f"Schema: {schema_str}\n\n"
            "Provide the SQL queries for the following requests:\n"
            f"{queries_section}\n"
        )
        return prompt
