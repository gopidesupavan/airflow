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
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Optional

from airflow.providers.common.sql.hooks.handlers import fetch_all_handler
from pydantic import BaseModel, Field
from pydantic_ai import Agent
from pydantic_ai.models.openai import OpenAIModel  # type: ignore[attr-defined]
from pydantic_ai.providers.github import GitHubProvider
from tabulate import tabulate

from airflow.providers.standard.datafusion.config import DataSourceConfig

from airflow.providers.standard.datafusion.engine import DataFusionEngine
from airflow.sdk import BaseHook, BaseOperator

if TYPE_CHECKING:
    from airflow.providers.standard.datafusion.config import DataSourceConfig


class SQLQueryResponse(BaseModel):
    """Response model for SQL query generation."""

    sql_queries: list[str] = Field(description="Provide the SQL queries based on the user prompt.")
    prompt: list[str] = Field(description="Set prompt for each query used to generate the query.")
    explanation: list[str] = Field(description="Explanation of the generated SQL queries.")
    result: Optional[Any] = Field(description="Optional field to hold execution results or additional data.",
                                  default=None)


class SchemaAnalysisResponse(BaseModel):
    """Response model for schema analysis and migration."""

    breaking_changes: list[str] = Field(description="List of breaking changes detected in the schema.")
    migration_queries: list[str] = Field(description="SQL migration queries to handle the breaking changes.")
    recommendations: list[str] = Field(description="Recommendations for handling the schema changes.")


class DataQualityAnalysisResponse(BaseModel):
    """Response model for data quality assessment."""

    status: str = Field(description="Provide statu PASS or FAIL based on the data quality assessment.")


class BaseLLMOperator(BaseOperator, ABC):
    """
    Base operator for LLM-powered operations.

    :param llm_model: The language model to use for generating responses.
    :param llm_conn_id: The Airflow connection ID for the LLM provider.
    :param datafusion_conn_id: The Airflow connection ID for DataFusion.
    """

    def __init__(
        self,
        data_sources: list,
        llm_model: str ="openai/gpt-4o", #"openai/gpt-4.1",
        llm_conn_id: str = "llm_default",
        prompt: str = "",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.llm_conn_id = llm_conn_id
        self.llm_model = llm_model
        self.data_sources = data_sources
        self.prompt = prompt
        self._agent = None

    def _get_llm_agent(self, output_type: type[BaseModel], instructions=None) -> Agent:
        if instructions is None:
            instructions = "You are a data engineer expert. write queries based on the prompt and the schema provided. return proper query"
        """Initialize and return the LLM agent."""
        if self._agent is None:
            connection = BaseHook.get_connection(self.llm_conn_id)

            github_provider = GitHubProvider(
                api_key=connection.password  # PAT token stored in Airflow connection
            )
            github_provider_model = OpenAIModel(
                model_name=self.llm_model,
                provider=github_provider,
            )

            self._agent = Agent(github_provider_model, output_type=output_type, instructions=instructions)

        return self._agent

    def _register_datasources(self) -> None:
        """Register data sources with DataFusion and retrieve their schemas."""
        if isinstance(self.data_sources, str):
            import json

            self.data_sources = json.loads(self.data_sources)

        for data_source in self.data_sources:
            self.engine.register_data_source(DataSourceConfig.model_validate(data_source))

    @abstractmethod
    def _prepare_prompt(self) -> str:
        """Prepare the prompt for the LLM based on the datasource and operation type."""
        pass

    @abstractmethod
    def _process_llm_response(self, response: Any) -> dict[str, Any]:
        """Process the LLM response and return formatted results."""
        pass

    def execute(self, context):
        """Execute the LLM operation."""
        # Register data sources with DataFusion and get schemas
        self.engine = DataFusionEngine()
        self._register_datasources()
        return self._run_with_agent()

    @abstractmethod
    def _run_with_agent(self):
        raise NotImplementedError

    @abstractmethod
    def _get_output_type(self) -> type[BaseModel]:
        """Return the output type for the LLM agent."""
        pass

    @abstractmethod
    def _get_table_headers(self) -> list[str]:
        """Return the headers for the results table."""
        pass

    @abstractmethod
    def _format_table_data(self, response: Any) -> list[tuple]:
        """Format the response data for table display."""
        pass

    def _get_db_api_hook(self, conn_id: str):
        """Get the database API hook for the given connection ID."""
        connection = BaseHook.get_connection(conn_id)
        return connection.get_hook(hook_params={})

    def _get_note(self):
        return """
        DO NOT GENERATE ANY DDL STATEMENTS THAT DROPS OR DELETE DATA.
        """


class LLMSchemaCompareOperator(BaseLLMOperator):
    """
    An operator that analyzes schema changes and generates migration queries using LLM.
    """

    def __init__(
        self,
        **kwargs,
    ) -> None:

        super().__init__(**kwargs)


    def _run_with_agent(self):
        agent = self._get_llm_agent(self._get_output_type())
        prompt = self._prepare_prompt()
        self.log.info("Prepared Prompt: %s", prompt)
        response = agent.run_sync(prompt)
        processed_result = self._process_llm_response(response)
        return {"view": self._format_table_data(processed_result), "migration_queries": json.dumps(processed_result.get("migration_queries", []))}


    def _prepare_prompt(self) -> str:
        t1_source_config = self.data_sources[0]
        t2_source_config = self.data_sources[1]
        if t1_source_config.table_name in self.engine.registered_tables:
            t1_schema = self.engine.execute_query(f"DESCRIBE {t1_source_config.table_name}")
        else:
            db_api_hook = self._get_db_api_hook(t1_source_config.connection_id)
            t1_schema = db_api_hook.get_schema(t1_source_config.table_name)

        if t2_source_config.table_name in self.engine.registered_tables:
            t2_schema = self.engine.execute_query(f"DESCRIBE {t2_source_config.table_name}")
        else:
            db_api_hook = self._get_db_api_hook(t2_source_config.connection_id)
            t2_schema = db_api_hook.get_schema(t2_source_config.table_name)

        prompt = f"""

        Downstream Table: {t1_source_config.table_name}
        Schema: {json.dumps(t1_schema)}

        Upstream Table: {t2_source_config.table_name}
        type: {t2_source_config.format_type.value}
        Schema: {json.dumps(t2_schema)}

        {self.prompt} \\n

        NOTE: {self._get_note()}
        """

        return prompt

    def _process_llm_response(self, response: Any) -> dict[str, Any]:
        return {
            "breaking_changes": response.output.breaking_changes,
            "migration_queries": response.output.migration_queries,
        }

    def _get_output_type(self) -> type[BaseModel]:
        return SchemaAnalysisResponse

    def _get_table_headers(self) -> list[str]:
        return ["Change Detected", "Migration Query"]

    def _format_table_data(self, response: Any):
        t1_source_config = self.data_sources[0]
        t2_source_config = self.data_sources[1]
        from tabulate import tabulate
        results_tables_str = ""
        breaking_changes = response.get("breaking_changes", [])
        migration_queries = response.get("migration_queries", [])
        data = list(zip(breaking_changes, migration_queries))
        headers = self._get_table_headers()

        markdown_table = tabulate(data, headers=headers, tablefmt="github")
        results_tables_str += f"\n### Schema Analysis between: {t1_source_config.table_name} and {t2_source_config.table_name}\n\n{markdown_table}\n\n{'-' * 40}\n"
        return results_tables_str


class LLMDataQualityOperator(BaseLLMOperator):
    """
    An operator that analyzes data quality issues and generates SQL queries using LLM.
    """

    def __init__(
        self,
        require_approval: bool = False,
        **kwargs,
    ) -> None:
        self.require_approval = require_approval
        super().__init__(**kwargs)


    def _run_with_agent(self):
        agent = self._get_llm_agent(self._get_output_type())
        prompt = self._prepare_prompt()
        self.log.info("Prepared Prompt: %s", prompt)
        response = agent.run_sync(prompt)
        print(response)
        processed_result = self._process_llm_response(response)
        results = self._run_dq_checks(processed_result.get("data_quality_queries", []), response.output.prompt)
        return {"view": self._format_table_data(results), "dq_results": json.dumps(results)}

    def _run_dq_checks(self, queries: list[str], prompts: list[str]) -> list[Any]:
        self._agent = None
        agent = self._get_llm_agent(DataQualityAnalysisResponse, instructions="You are a data engineer expert. based on the results provide PASS or FAIL status for the data quality check")

        results = []
        for dq_query, prmt in zip(queries, prompts):
            dq_result = self.engine.execute_query(dq_query)
            prompt = f"""
                for the {prmt} and the results are {json.dumps(dq_result)}

                Determine the data quality status based on the above results:
            """
            rs = agent.run_sync(prompt)

            results.append({
                "prompt": prmt,
                "query": dq_query,
                "result": dq_result,
                "status": rs.output.status
            })
        return results


    def _prepare_prompt(self) -> str:
        t1_source_config = self.data_sources[0]
        if t1_source_config.table_name in self.engine.registered_tables:
            t1_schema = self.engine.execute_query(f"DESCRIBE {t1_source_config.table_name}")

        prompt = f"""

        Table: {t1_source_config.table_name}
        Schema: {json.dumps(t1_schema)}

        {self.prompt} \n

        NOTE: {self._get_note()}
        """
        return prompt

    def _process_llm_response(self, response: Any) -> dict[str, Any]:
        return {
            "data_quality_queries": response.output.sql_queries,
        }

    def _get_output_type(self) -> type[BaseModel]:
        return SQLQueryResponse

    def _get_table_headers(self):
        return ["Query", "Result"]

    def _format_table_data(self, results_fmt: Any):
        if results_fmt:

            # Define the desired header order
            headers = ["prompt", "query", "result", "status"]

            # Normalize results to ensure all keys are present and follow the header order
            normalized_results = [
                {key: item.get(key, None) for key in headers} for item in results_fmt
            ]

            rows = [[item[key] for key in headers] for item in normalized_results]

            # Format the table using the specified headers and rows
            github_formatted_output = tabulate(rows, headers=headers, tablefmt="github")
            return github_formatted_output
        else:
            print("No results to display.")

