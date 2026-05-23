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

from unittest.mock import MagicMock, patch

import pytest

from airflow.providers.common.ai.hooks.base_ai import (
    AgentRunRequest,
    AgentRunResult,
    AgentUsage,
    BaseAIHook,
    BaseToolset,
    DurableContext,
    DurableStats,
    ToolSpec,
)
from airflow.providers.common.compat.sdk import BaseHook


class TestBaseAIHookGetAgentHook:
    @patch("airflow.providers.common.ai.hooks.base_ai.BaseHook.get_hook", autospec=True)
    def test_returns_hook_when_instance_is_base_ai_hook(self, mock_get_hook):
        mock_hook = MagicMock(spec=BaseAIHook)
        mock_get_hook.return_value = mock_hook

        result = BaseAIHook.get_agent_hook("my_conn")

        assert result is mock_hook
        mock_get_hook.assert_called_once_with("my_conn", hook_params=None)

    @patch("airflow.providers.common.ai.hooks.base_ai.BaseHook.get_hook", autospec=True)
    def test_raises_when_hook_is_not_base_ai_hook(self, mock_get_hook):
        mock_get_hook.return_value = MagicMock(spec=BaseHook)

        with pytest.raises(TypeError, match="not a BaseAIHook"):
            BaseAIHook.get_agent_hook("my_conn")


class TestAgentRunResult:
    def test_dataclass_fields(self):
        usage = AgentUsage(requests=1, tool_calls=2, total_tokens=10)
        result = AgentRunResult(
            output="answer",
            message_history=["msg"],
            model_name="test-model",
            usage=usage,
            tool_names=["query"],
        )
        assert result.output == "answer"
        assert result.message_history == ["msg"]
        assert result.model_name == "test-model"
        assert result.usage == usage
        assert result.tool_names == ["query"]

    def test_durable_stats_defaults_to_none(self):
        result = AgentRunResult(output="x")
        assert result.durable_stats is None


class TestToolSpec:
    def test_fields(self):
        fn = lambda: "result"  # noqa: E731
        spec = ToolSpec(
            name="my_tool",
            description="Does something.",
            parameters={"type": "object", "properties": {}},
            fn=fn,
        )
        assert spec.name == "my_tool"
        assert spec.description == "Does something."
        assert spec.fn is fn


class TestDurableContext:
    def test_fields(self):
        ctx = DurableContext(dag_id="d", task_id="t", run_id="r", map_index=2)
        assert ctx.dag_id == "d"
        assert ctx.map_index == 2

    def test_map_index_defaults_to_minus_one(self):
        ctx = DurableContext(dag_id="d", task_id="t", run_id="r")
        assert ctx.map_index == -1


class TestDurableStats:
    def test_defaults(self):
        s = DurableStats()
        assert s.replayed_model == 0
        assert s.replayed_tool == 0
        assert s.cached_model == 0
        assert s.cached_tool == 0


class TestAgentRunRequest:
    def test_required_prompt(self):
        req = AgentRunRequest(prompt="hello")
        assert req.prompt == "hello"
        assert req.output_type is str
        assert req.enable_tool_logging is True
        assert req.toolsets is None
        assert req.durable_context is None

    def test_all_fields(self):
        ctx = DurableContext(dag_id="d", task_id="t", run_id="r")
        req = AgentRunRequest(
            prompt="test",
            output_type=dict,
            instructions="Be helpful.",
            toolsets=[MagicMock()],
            usage_limits=MagicMock(),
            enable_tool_logging=False,
            durable_context=ctx,
            agent_params={"retries": 3},
        )
        assert req.instructions == "Be helpful."
        assert req.enable_tool_logging is False
        assert req.durable_context is ctx


class TestBaseToolset:
    def test_subclass_must_implement_as_tools(self):
        with pytest.raises(TypeError):
            BaseToolset()  # abstract

    def test_concrete_subclass(self):
        class MyToolset(BaseToolset):
            def as_tools(self):
                return []

        ts = MyToolset()
        assert ts.as_tools() == []


class TestBaseAIHookLoggedCallable:
    def test_logged_callable_preserves_result(self):
        import logging

        logger = MagicMock(spec=logging.Logger)
        logger.isEnabledFor.return_value = True

        def adder(x: int, y: int) -> int:
            return x + y

        wrapped = BaseAIHook._logged_callable(adder, logger)
        result = wrapped(x=1, y=2)

        assert result == 3
        logger.info.assert_called()

    def test_logged_callable_preserves_signature(self):
        import inspect

        def greet(name: str) -> str:
            """Say hello."""
            return f"Hello {name}"

        mock_log = MagicMock()
        mock_log.isEnabledFor.return_value = False
        wrapped = BaseAIHook._logged_callable(greet, mock_log)

        sig = inspect.signature(wrapped)
        assert "name" in sig.parameters

    def test_logged_callable_re_raises_exceptions(self):
        import logging

        logger = MagicMock(spec=logging.Logger)

        def boom(**kwargs):
            raise RuntimeError("fail")

        wrapped = BaseAIHook._logged_callable(boom, logger)
        with pytest.raises(RuntimeError, match="fail"):
            wrapped()


class TestBaseAIHookCachedCallable:
    def test_cached_callable_returns_cached_result_on_hit(self):
        storage = MagicMock()
        counter = MagicMock()
        counter.next_step.return_value = 0
        storage.load_tool_result.return_value = (True, "cached_value")

        def fn(**kwargs):
            return "fresh_value"

        wrapped = BaseAIHook._cached_callable(fn, storage, counter)
        result = wrapped()

        assert result == "cached_value"
        counter.next_step.assert_called_once()
        storage.load_tool_result.assert_called_once_with("tool_step_0")
        storage.save_tool_result.assert_not_called()

    def test_cached_callable_executes_and_caches_on_miss(self):
        storage = MagicMock()
        counter = MagicMock()
        counter.next_step.return_value = 1
        storage.load_tool_result.return_value = (False, None)

        def fn(**kwargs):
            return "fresh_value"

        wrapped = BaseAIHook._cached_callable(fn, storage, counter)
        result = wrapped()

        assert result == "fresh_value"
        storage.save_tool_result.assert_called_once_with("tool_step_1", "fresh_value")
        assert counter.cached_tool == 1
