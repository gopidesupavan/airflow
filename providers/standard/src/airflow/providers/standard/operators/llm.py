from airflow.sdk import BaseOperator
from typing import List

from pydantic import BaseModel, Field
from pydantic_ai import Agent
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.providers.ollama import OllamaProvider

class SQLQuery(BaseModel):
    sql_query: str = Field(description="The SQL query constructed based on the schema and the user prompt")

class QueryGeneratorOperator(BaseOperator):
    """
    An operator that generates a query based on a prompt using a language model (LLM).

    :param prompt: The prompt to generate the query from.
    :param llm_model: The language model to use for generating the query.
    """

    def __init__(self, prompt: str,
                 llm_model: str = "llama3.2",
                 llm_conn_id: str = "llm_default",
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self.prompt = prompt
        self.llm_model = llm_model

    def execute(self, context):
        ollama_model = OpenAIChatModel(
            model_name=self.llm_model,
            provider=OllamaProvider(base_url="http://host.docker.internal:11434/v1")
        )

        agent = Agent(ollama_model, output_type=SQLQuery)

        result = agent.run_sync(self.prompt)
        return result.output.sql_query
