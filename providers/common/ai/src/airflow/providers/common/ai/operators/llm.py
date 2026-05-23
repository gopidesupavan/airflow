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
"""Operator for general-purpose LLM calls."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import timedelta
from functools import cached_property
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

from airflow.providers.common.ai.hooks.base_ai import AgentRunRequest, BaseAIHook
from airflow.providers.common.ai.mixins.approval import LLMApprovalMixin
from airflow.providers.common.ai.utils.logging import log_run_summary
from airflow.providers.common.compat.sdk import BaseOperator

if TYPE_CHECKING:
    from airflow.sdk import Context


class LLMOperator(BaseOperator, LLMApprovalMixin):
    """
    Call an LLM with a prompt and return the output.

    Uses a :class:`~airflow.providers.common.ai.hooks.base_ai.BaseAIHook`
    for LLM access. The backend is selected by the connection ``conn_type``.
    Supports plain string output (default) and structured output via a Pydantic
    ``BaseModel``. When ``output_type`` is a ``BaseModel`` subclass, the result
    is serialized via ``model_dump()`` for XCom.

    :param prompt: The prompt to send to the LLM.
    :param llm_conn_id: Connection ID for the LLM provider.
    :param model_id: Model identifier (e.g. ``"openai:gpt-5"``).
        Overrides the model stored in the connection's extra field.
    :param system_prompt: System-level instructions for the LLM agent.
    :param output_type: Expected output type. Default ``str``. Set to a Pydantic
        ``BaseModel`` subclass for structured output.
    :param agent_params: Additional keyword arguments passed to the underlying
        agent constructor (e.g. pydantic-ai ``retries``, ``model_settings``, ``tools``).
    :param usage_limits: Backend-specific usage limits. Pass
        ``UsageLimits(request_limit=..., total_tokens_limit=..., ...)`` to fail
        the task when the agent exceeds the configured budget.
        ``None`` (default) means no enforcement.
    :param require_approval: If ``True``, the task defers after generating
        output and waits for a human reviewer to approve or reject via the
        HITL interface.  Default ``False``.
    :param approval_timeout: Maximum time to wait for a review.  When
        exceeded, the task fails with ``TimeoutError``.
    :param allow_modifications: If ``True``, the reviewer can edit the output
        before approving.  The modified value is returned as the task result.
        Default ``False``.
    """

    template_fields: Sequence[str] = (
        "prompt",
        "llm_conn_id",
        "model_id",
        "system_prompt",
        "agent_params",
    )

    def __init__(
        self,
        *,
        prompt: str,
        llm_conn_id: str,
        model_id: str | None = None,
        system_prompt: str = "",
        output_type: type = str,
        agent_params: dict[str, Any] | None = None,
        usage_limits: Any = None,
        require_approval: bool = False,
        approval_timeout: timedelta | None = None,
        allow_modifications: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.prompt = prompt
        self.llm_conn_id = llm_conn_id
        self.model_id = model_id
        self.system_prompt = system_prompt
        self.output_type = output_type
        self.agent_params = agent_params or {}
        self.usage_limits = usage_limits
        self.require_approval = require_approval
        self.approval_timeout = approval_timeout
        self.allow_modifications = allow_modifications

    @cached_property
    def llm_hook(self) -> BaseAIHook:
        """Return the agent hook for the configured connection (resolved from ``conn_type``)."""
        hook_params = {"model_id": self.model_id}
        return BaseAIHook.get_agent_hook(self.llm_conn_id, hook_params=hook_params)

    def execute(self, context: Context) -> Any:
        request = AgentRunRequest(
            prompt=self.prompt,
            output_type=self.output_type,
            instructions=self.system_prompt,
            usage_limits=self.usage_limits,
            agent_params=self.agent_params,
        )
        result = self.llm_hook.execute_agent(request)
        log_run_summary(self.log, result)
        output = result.output

        if self.require_approval:
            self.defer_for_approval(context, output)  # type: ignore[misc]

        if isinstance(output, BaseModel):
            output = output.model_dump()

        return output
