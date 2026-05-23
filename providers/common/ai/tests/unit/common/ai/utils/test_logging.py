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

import logging
from unittest.mock import MagicMock

from pydantic import BaseModel

from airflow.providers.common.ai.hooks.base_ai import AgentRunResult, AgentUsage
from airflow.providers.common.ai.utils.logging import _log_output_debug, log_run_summary


class TestLogRunSummary:
    def test_logs_usage_and_endgroup(self):
        logger = MagicMock(spec=logging.Logger)
        result = AgentRunResult(
            output="done",
            model_name="gpt-5",
            usage=AgentUsage(
                requests=4, tool_calls=3, input_tokens=2847, output_tokens=512, total_tokens=3359
            ),
        )

        log_run_summary(logger, result)

        first_call_args = logger.info.call_args_list[0][0]
        assert "::group::" in first_call_args[0]
        assert "gpt-5" in first_call_args[1:]
        assert 4 in first_call_args[1:]
        assert 3 in first_call_args[1:]
        # endgroup is the last info call
        logger.info.assert_called_with("::endgroup::")

    def test_logs_tool_sequence_when_present(self):
        logger = MagicMock(spec=logging.Logger)
        result = AgentRunResult(
            output="done",
            model_name="gpt-5",
            tool_names=["list_tables", "get_schema", "query"],
        )

        log_run_summary(logger, result)

        calls = [str(c) for c in logger.info.call_args_list]
        assert any("list_tables -> get_schema -> query" in c for c in calls)

    def test_no_tool_sequence_when_absent(self):
        logger = MagicMock(spec=logging.Logger)
        result = AgentRunResult(output="done", model_name="gpt-5")

        log_run_summary(logger, result)

        calls = [str(c) for c in logger.info.call_args_list]
        assert not any("Tool call sequence:" in c for c in calls)

    def test_no_usage_falls_back_to_simple_header(self):
        logger = MagicMock(spec=logging.Logger)
        result = AgentRunResult(output="done", model_name="mymodel")

        log_run_summary(logger, result)

        first_call_args = logger.info.call_args_list[0][0]
        assert "::group::" in first_call_args[0]
        assert "mymodel" in first_call_args[1:]


class TestLogOutputDebug:
    def test_logs_string_output(self):
        logger = MagicMock(spec=logging.Logger)
        logger.isEnabledFor.return_value = True

        _log_output_debug(logger, "Hello world")

        logger.debug.assert_called_once()
        assert "Hello world" in str(logger.debug.call_args)

    def test_logs_pydantic_model_dump(self):
        class Info(BaseModel):
            name: str

        logger = MagicMock(spec=logging.Logger)
        logger.isEnabledFor.return_value = True

        _log_output_debug(logger, Info(name="Alice"))

        logger.debug.assert_called_once()
        assert "Alice" in str(logger.debug.call_args)

    def test_truncates_long_output(self):
        logger = MagicMock(spec=logging.Logger)
        logger.isEnabledFor.return_value = True
        long_text = "x" * 1000

        _log_output_debug(logger, long_text)

        call_str = str(logger.debug.call_args)
        assert "..." in call_str

    def test_skipped_when_debug_disabled(self):
        logger = MagicMock(spec=logging.Logger)
        logger.isEnabledFor.return_value = False

        _log_output_debug(logger, "should not appear")

        logger.debug.assert_not_called()
