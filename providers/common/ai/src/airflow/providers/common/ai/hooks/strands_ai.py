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
"""Hooks for LLM agents via the Strands Agents SDK."""

from __future__ import annotations

import functools
from abc import abstractmethod
from typing import Any

from airflow.providers.common.ai.hooks.base_ai import AgentRunRequest, AgentRunResult, BaseAIHook, ToolSpec


class StrandsHook(BaseAIHook):
    """
    Base hook for LLM agents via `Strands Agents <https://strandsagents.com/>`__.

    Subclasses implement :meth:`get_conn` to return a configured Strands model instance
    (e.g. :class:`strands.models.BedrockModel`, :class:`strands.models.AnthropicModel`,
    :class:`strands.models.litellm.LiteLLMModel`). The :meth:`execute_agent` and
    :meth:`_spec_to_native` implementations are shared across all Strands model backends.
    """

    conn_name_attr = "llm_conn_id"
    default_conn_name = "strands_default"

    supports_toolsets = True
    supports_durable = False
    supports_usage_limits = False

    def __init__(
        self,
        llm_conn_id: str | None = None,
        model_id: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.llm_conn_id = llm_conn_id if llm_conn_id is not None else self.default_conn_name
        self.model_id = model_id

    @abstractmethod
    def get_model(self) -> Any:
        """Return a configured Strands model instance."""

    # ------------------------------------------------------------------
    # BaseAIHook abstract interface — shared across all Strands models
    # ------------------------------------------------------------------

    def _spec_to_native(self, spec: ToolSpec) -> Any:
        """Convert a :class:`~airflow.providers.common.ai.hooks.base_ai.ToolSpec` to a Strands tool."""
        try:
            from strands import tool as strands_tool
        except ImportError as e:
            from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

            raise AirflowOptionalProviderFeatureException(
                "The 'strands-agents' package is required for StrandsHook. "
                "Install it with: pip install strands-agents"
            ) from e

        fn = spec.fn

        # Strands infers tool name from __name__ and description from __doc__.
        # functools.wraps preserves __wrapped__ so inspect.signature() follows it
        # for parameter schema inference, then we override name/doc from spec.
        @functools.wraps(fn)
        def tool_fn(*args: Any, **kwargs: Any) -> Any:
            return fn(*args, **kwargs)

        tool_fn.__name__ = spec.name.replace("-", "_")
        tool_fn.__doc__ = spec.description
        return strands_tool(tool_fn)

    def create_agent(self, request: AgentRunRequest) -> Any:
        """Build a Strands ``Agent`` from *request*."""
        try:
            from strands import Agent
        except ImportError as e:
            from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

            raise AirflowOptionalProviderFeatureException(
                "The 'strands-agents' package is required for StrandsHook. "
                "Install it with: pip install strands-agents"
            ) from e

        native_tools: list[Any] = []
        if request.toolsets:
            native_tools = self._resolve_tools(
                request.toolsets,
                request.enable_tool_logging,
                None,  # durable execution is not supported for Strands
                None,
            )

        agent_kwargs: dict[str, Any] = dict(request.agent_params or {})
        if request.instructions:
            agent_kwargs["system_prompt"] = request.instructions

        return Agent(model=self.get_model(), tools=native_tools or [], **agent_kwargs)

    def run_agent(self, agent: Any, request: AgentRunRequest) -> AgentRunResult:
        """Run the Strands *agent* for *request* and return a normalized :class:`~airflow.providers.common.ai.hooks.base_ai.AgentRunResult`."""
        response = agent(request.prompt)
        return AgentRunResult(
            output=str(response),
            model_name=self.model_id,
        )

    def test_connection(self) -> tuple[bool, str]:
        """Validate the connection by instantiating the model (no API call)."""
        try:
            self.get_model()
            return True, f"{type(self).__name__} resolved successfully."
        except Exception as e:
            return False, str(e)


class StrandsBedrockHook(StrandsHook):
    """
    Hook for Strands Agents using AWS Bedrock as the model backend.

    Credentials are resolved in order:

    1. IAM keys from ``extra`` (``aws_access_key_id`` + ``aws_secret_access_key``,
       optionally ``aws_session_token``).
    2. Environment-variable / instance-role chain (``AWS_PROFILE``, IAM role, …)
       when no explicit keys are provided.

    Connection fields:
        - **extra** JSON::

            {
                "model": "us.anthropic.claude-opus-4-5-20251101",
                "region_name": "us-east-1",
                "aws_access_key_id": "AKIA...",
                "aws_secret_access_key": "...",
                "aws_session_token": "...",
            }

    :param llm_conn_id: Airflow connection ID.
    :param model_id: Bedrock model identifier (e.g. ``"us.anthropic.claude-opus-4-5-20251101"``).
        Overrides the model stored in the connection's extra field.
    """

    conn_type = "strands-bedrock"
    hook_name = "Strands Bedrock"
    default_conn_name = "strands_bedrock_default"

    @staticmethod
    def get_ui_field_behaviour() -> dict[str, Any]:
        """Return custom field behaviour for the Airflow connection form."""
        return {
            "hidden_fields": ["schema", "port", "login", "host", "password"],
            "relabeling": {},
            "placeholders": {
                "extra": (
                    '{"model": "us.anthropic.claude-opus-4-5-20251101", "region_name": "us-east-1"}'
                    "  — leave aws_access_key_id empty for IAM role / env-var auth"
                ),
            },
        }

    def get_model(self) -> Any:
        """
        Return a configured Strands ``BedrockModel``.

        Resolution order:

        1. **Explicit IAM credentials** — when ``aws_access_key_id`` / ``aws_secret_access_key``
           are present in the connection's extra JSON.
        2. **Default resolution** — delegates to boto3's credential chain
           (``AWS_PROFILE``, instance role, ``AWS_ACCESS_KEY_ID`` env var, …).
        """
        try:
            from strands.models import BedrockModel
        except ImportError as e:
            from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

            raise AirflowOptionalProviderFeatureException(
                "The 'strands-agents' package is required for StrandsBedrockHook. "
                "Install it with: pip install strands-agents"
            ) from e

        conn = self.get_connection(self.llm_conn_id)
        extra: dict[str, Any] = conn.extra_dejson
        model_id: str = self.model_id or extra.get("model", "")
        if not model_id:
            raise ValueError(
                "No model specified. Set model_id on the hook or the 'model' field in the connection extra."
            )

        kwargs: dict[str, Any] = {"model_id": model_id}

        if extra.get("region_name"):
            kwargs["region_name"] = extra["region_name"]

        if extra.get("aws_access_key_id") and extra.get("aws_secret_access_key"):
            import boto3

            session_kwargs: dict[str, Any] = {
                "aws_access_key_id": extra["aws_access_key_id"],
                "aws_secret_access_key": extra["aws_secret_access_key"],
            }
            if extra.get("aws_session_token"):
                session_kwargs["aws_session_token"] = extra["aws_session_token"]
            if extra.get("region_name"):
                session_kwargs["region_name"] = extra["region_name"]

            boto_session = boto3.Session(**session_kwargs)
            kwargs["boto_session"] = boto_session

        self.log.info("Creating Strands BedrockModel: model_id=%s", model_id)
        return BedrockModel(**kwargs)
