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
from unittest.mock import MagicMock, patch

import pytest

from airflow.models.connection import Connection
from airflow.providers.common.ai.hooks.base_ai import (
    AgentRunRequest,
    AgentRunResult,
    BaseAIHook,
    ToolSpec,
)
from airflow.providers.common.ai.hooks.strands_ai import StrandsBedrockHook, StrandsHook


class TestStrandsHookContract:
    def test_strands_hook_is_abstract(self):
        """StrandsHook cannot be instantiated — get_conn is abstract."""
        with pytest.raises(TypeError):
            StrandsHook()  # type: ignore[abstract]

    def test_strands_bedrock_hook_is_base_ai_hook(self):
        assert issubclass(StrandsBedrockHook, BaseAIHook)

    def test_strands_bedrock_hook_is_strands_hook(self):
        assert issubclass(StrandsBedrockHook, StrandsHook)

    def test_capability_flags(self):
        assert StrandsBedrockHook.supports_toolsets is True
        assert StrandsBedrockHook.supports_durable is False
        assert StrandsBedrockHook.supports_usage_limits is False

    def test_conn_type(self):
        assert StrandsBedrockHook.conn_type == "strands-bedrock"


class TestStrandsBedrockHookInit:
    def test_default_conn_id(self):
        hook = StrandsBedrockHook()
        assert hook.llm_conn_id == "strands_bedrock_default"
        assert hook.model_id is None

    def test_custom_conn_id_and_model(self):
        hook = StrandsBedrockHook(llm_conn_id="my_conn", model_id="us.anthropic.claude-opus-4-5")
        assert hook.llm_conn_id == "my_conn"
        assert hook.model_id == "us.anthropic.claude-opus-4-5"


class TestStrandsBedrockHookGetModel:
    @patch("airflow.providers.common.ai.hooks.strands_ai.StrandsBedrockHook.get_connection")
    def test_get_model_with_env_auth(self, mock_get_connection):
        """No explicit credentials — delegates to boto3 credential chain."""
        mock_get_connection.return_value = Connection(
            conn_id="test",
            conn_type="strands-bedrock",
            extra=json.dumps({"model": "us.anthropic.claude-opus-4-5-20251101"}),
        )

        hook = StrandsBedrockHook(llm_conn_id="test", model_id="us.anthropic.claude-opus-4-5-20251101")
        mock_bedrock_model = MagicMock()

        with patch.dict("sys.modules", {"strands": MagicMock(), "strands.models": MagicMock()}):
            import strands.models

            strands.models.BedrockModel = MagicMock(return_value=mock_bedrock_model)
            result = hook.get_model()

        assert result is mock_bedrock_model

    @patch("airflow.providers.common.ai.hooks.strands_ai.StrandsBedrockHook.get_connection")
    def test_get_model_with_explicit_iam_keys(self, mock_get_connection):
        """Explicit IAM keys create a boto3 Session and pass it to BedrockModel."""
        mock_get_connection.return_value = Connection(
            conn_id="test",
            conn_type="strands-bedrock",
            extra=json.dumps(
                {
                    "model": "us.anthropic.claude-opus-4-5-20251101",
                    "region_name": "us-east-1",
                    "aws_access_key_id": "AKIA123",
                    "aws_secret_access_key": "secret",
                }
            ),
        )
        hook = StrandsBedrockHook(llm_conn_id="test")
        mock_bedrock_model = MagicMock()
        mock_boto_session = MagicMock()

        with (
            patch.dict("sys.modules", {"strands": MagicMock(), "strands.models": MagicMock()}),
            patch("boto3.Session", return_value=mock_boto_session) as mock_session,
        ):
            import strands.models

            strands.models.BedrockModel = MagicMock(return_value=mock_bedrock_model)
            hook.get_model()

        mock_session.assert_called_once_with(
            aws_access_key_id="AKIA123",
            aws_secret_access_key="secret",
            region_name="us-east-1",
        )
        strands.models.BedrockModel.assert_called_once()
        call_kwargs = strands.models.BedrockModel.call_args[1]
        assert call_kwargs["boto_session"] is mock_boto_session

    @patch("airflow.providers.common.ai.hooks.strands_ai.StrandsBedrockHook.get_connection")
    def test_get_model_raises_when_no_model(self, mock_get_connection):
        mock_get_connection.return_value = Connection(
            conn_id="test",
            conn_type="strands-bedrock",
            extra=json.dumps({}),
        )
        hook = StrandsBedrockHook(llm_conn_id="test")

        with (
            patch.dict("sys.modules", {"strands": MagicMock(), "strands.models": MagicMock()}),
            pytest.raises(ValueError, match="No model specified"),
        ):
            hook.get_model()

    def test_get_model_raises_on_missing_strands(self):
        hook = StrandsBedrockHook(llm_conn_id="test", model_id="any")
        with patch.dict("sys.modules", {"strands": None, "strands.models": None}):
            with pytest.raises(ImportError):
                hook.get_model()


class TestStrandsHookSpecToNative:
    def test_spec_to_native_wraps_fn_as_strands_tool(self):
        """_spec_to_native calls strands.tool() on a functools.wraps wrapper."""
        mock_strands_tool = MagicMock(side_effect=lambda fn: fn)

        def my_fn(x: int) -> str:
            """Does x."""
            return str(x)

        spec = ToolSpec(
            name="my_fn",
            description="Does x.",
            parameters={"type": "object", "properties": {"x": {"type": "integer"}}},
            fn=my_fn,
        )

        hook = StrandsBedrockHook.__new__(StrandsBedrockHook)

        with patch.dict("sys.modules", {"strands": MagicMock(tool=mock_strands_tool)}):
            import sys

            sys.modules["strands"].tool = mock_strands_tool
            hook._spec_to_native(spec)

        mock_strands_tool.assert_called_once()
        wrapped = mock_strands_tool.call_args[0][0]
        assert wrapped.__name__ == "my_fn"
        assert wrapped.__doc__ == "Does x."

    def test_spec_to_native_sanitises_name(self):
        """Hyphens in tool name are replaced with underscores for Python identifiers."""
        mock_strands_tool = MagicMock(side_effect=lambda fn: fn)
        spec = ToolSpec(
            name="my-tool",
            description="desc",
            parameters={},
            fn=lambda: None,
        )

        hook = StrandsBedrockHook.__new__(StrandsBedrockHook)

        with patch.dict("sys.modules", {"strands": MagicMock(tool=mock_strands_tool)}):
            import sys

            sys.modules["strands"].tool = mock_strands_tool
            hook._spec_to_native(spec)

        wrapped = mock_strands_tool.call_args[0][0]
        assert wrapped.__name__ == "my_tool"


class TestStrandsHookCreateAndRunAgent:
    def test_create_agent_builds_strands_agent(self):
        mock_agent_cls = MagicMock()
        mock_model = MagicMock()

        hook = StrandsBedrockHook.__new__(StrandsBedrockHook)
        hook.model_id = "us.anthropic.claude-3-test"
        hook.log = MagicMock()

        with patch.object(hook, "get_model", return_value=mock_model):
            with patch.dict("sys.modules", {"strands": MagicMock(Agent=mock_agent_cls)}):
                import sys

                sys.modules["strands"].Agent = mock_agent_cls

                request = AgentRunRequest(prompt="hello")
                agent = hook.create_agent(request)

        mock_agent_cls.assert_called_once()
        assert agent is mock_agent_cls.return_value

    def test_run_agent_returns_agent_run_result(self):
        mock_response = MagicMock()
        mock_response.__str__ = MagicMock(return_value="the answer")
        mock_agent_instance = MagicMock(return_value=mock_response)

        hook = StrandsBedrockHook.__new__(StrandsBedrockHook)
        hook.model_id = "us.anthropic.claude-3-test"
        hook.log = MagicMock()

        request = AgentRunRequest(prompt="hello")
        result = hook.run_agent(mock_agent_instance, request)

        assert isinstance(result, AgentRunResult)
        assert result.output == "the answer"
        assert result.model_name == "us.anthropic.claude-3-test"
        mock_agent_instance.assert_called_once_with("hello")

    def test_execute_agent_passes_instructions_as_system_prompt(self):
        mock_agent_cls = MagicMock()
        mock_agent_cls.return_value = MagicMock(return_value=MagicMock(__str__=lambda s: "ok"))
        mock_model = MagicMock()

        hook = StrandsBedrockHook.__new__(StrandsBedrockHook)
        hook.model_id = "m"
        hook.log = MagicMock()

        with patch.object(hook, "get_model", return_value=mock_model):
            with patch.dict("sys.modules", {"strands": MagicMock(Agent=mock_agent_cls)}):
                import sys

                sys.modules["strands"].Agent = mock_agent_cls

                request = AgentRunRequest(prompt="q", instructions="Be concise.")
                hook.create_agent(request)

        _, kwargs = mock_agent_cls.call_args
        assert kwargs.get("system_prompt") == "Be concise."

    def test_create_agent_resolves_toolsets(self):
        mock_agent_cls = MagicMock()
        mock_model = MagicMock()

        hook = StrandsBedrockHook.__new__(StrandsBedrockHook)
        hook.model_id = "m"
        hook.log = MagicMock()

        toolset = MagicMock()
        spec = ToolSpec(name="t", description="d", parameters={}, fn=lambda: None)
        toolset.as_tools.return_value = [spec]

        with patch.object(hook, "get_model", return_value=mock_model):
            with patch.object(hook, "_spec_to_native", return_value="native_tool") as mock_native:
                with patch.dict("sys.modules", {"strands": MagicMock(Agent=mock_agent_cls)}):
                    import sys

                    sys.modules["strands"].Agent = mock_agent_cls

                    request = AgentRunRequest(prompt="q", toolsets=[toolset], enable_tool_logging=False)
                    hook.create_agent(request)

        mock_native.assert_called_once()
        _, kwargs = mock_agent_cls.call_args
        assert kwargs.get("tools") == ["native_tool"]
