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

.. _howto/connection:strands-bedrock:

Strands Bedrock connection
===========================

The ``strands-bedrock`` connection type stores credentials and model configuration for
:class:`~airflow.providers.common.ai.hooks.strands_ai.StrandsBedrockHook`.

Connection fields
-----------------

All fields are stored in the connection's **Extra** JSON. No standard ``host``,
``login``, or ``password`` fields are used.

.. list-table::
   :header-rows: 1
   :widths: 20 10 70

   * - Field
     - Required
     - Description
   * - ``model``
     - Yes
     - Bedrock model identifier, e.g. ``us.anthropic.claude-opus-4-5-20251101``.
       Can be overridden per-run via the ``model_id`` hook parameter.
   * - ``region_name``
     - No
     - AWS region, e.g. ``us-east-1``. Falls back to the ``AWS_DEFAULT_REGION``
       environment variable or the boto3 default.
   * - ``aws_access_key_id``
     - No
     - IAM access key ID. Leave empty to use the instance role or environment
       credential chain.
   * - ``aws_secret_access_key``
     - No
     - IAM secret access key (required when ``aws_access_key_id`` is set).
   * - ``aws_session_token``
     - No
     - Temporary session token for short-lived IAM credentials (optional).

Example Extra JSON
------------------

.. code-block:: json

    {
        "model": "us.anthropic.claude-opus-4-5-20251101",
        "region_name": "us-east-1",
        "aws_access_key_id": "AKIAIOSFODNN7EXAMPLE",
        "aws_secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
    }

For EC2/ECS instance roles or environment-variable credentials, omit the IAM key
fields:

.. code-block:: json

    {
        "model": "us.anthropic.claude-opus-4-5-20251101",
        "region_name": "us-east-1"
    }
