 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

.. _howto/hook:strands_ai:

StrandsHook / StrandsBedrockHook
=================================

Use :class:`~airflow.providers.common.ai.hooks.strands_ai.StrandsHook` (or its concrete
subclass :class:`~airflow.providers.common.ai.hooks.strands_ai.StrandsBedrockHook`) to run
LLM agents via the `Strands Agents SDK <https://strandsagents.com/>`__.

``StrandsHook`` is an abstract base class that implements the shared :meth:`execute_agent`
and :meth:`_spec_to_native` logic for all Strands model backends. Subclasses implement
:meth:`get_conn` to return a specific Strands model instance (e.g. ``BedrockModel``,
``AnthropicModel``, ``LiteLLMModel``).

``StrandsBedrockHook`` is the built-in concrete subclass for the AWS Bedrock backend.

.. seealso::
    :ref:`Connection configuration <howto/connection:strands-bedrock>`

AWS Bedrock backend
-------------------

``StrandsBedrockHook`` reads the Bedrock model ID and AWS credentials from the
``strands-bedrock`` Airflow connection. Credentials are resolved in order:

1. IAM access key + secret from the connection's ``extra`` JSON
   (``aws_access_key_id``, ``aws_secret_access_key``, optionally ``aws_session_token``).
2. Standard boto3 credential chain — ``AWS_PROFILE`` env var, EC2/ECS instance role,
   ``~/.aws/credentials``, etc.

Use ``AgentOperator`` with a ``strands-bedrock`` connection:

.. code-block:: python

    from airflow.providers.common.ai.operators.agent import AgentOperator

    summarise = AgentOperator(
        task_id="summarise",
        llm_conn_id="strands_bedrock_default",
        prompt="Summarise the following text: {{ ti.xcom_pull('previous') }}",
    )

Custom Strands model backends
------------------------------

To use a different Strands model backend, subclass ``StrandsHook`` and implement
:meth:`get_conn`:

.. code-block:: python

    from airflow.providers.common.ai.hooks.strands_ai import StrandsHook


    class StrandsAnthropicHook(StrandsHook):
        conn_type = "my-strands-anthropic"
        hook_name = "Strands Anthropic"
        default_conn_name = "strands_anthropic_default"

        def get_conn(self):
            from strands.models import AnthropicModel

            conn = self.get_connection(self.llm_conn_id)
            return AnthropicModel(
                api_key=conn.password,
                model=conn.extra_dejson.get("model", "claude-opus-4-5"),
            )

Register the subclass in your provider's ``provider.yaml`` (or use
:meth:`~airflow.providers.common.ai.hooks.base_ai.BaseAIHook.get_agent_hook` directly).
