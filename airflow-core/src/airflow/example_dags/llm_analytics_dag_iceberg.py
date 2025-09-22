from airflow.sdk import DAG, task
from airflow.providers.standard.operators.llm import LLMQueryGeneratorOperator
from airflow.providers.standard.operators.analytics import AnalyticsOperator
from airflow.providers.standard.operators.hitl import ApprovalOperator
from airflow.providers.standard.datafusion.config import DataSourceConfig, FormatType
import datetime

with DAG(
    dag_id="llm_analytics_dag_with_iceberg",
    start_date=None,
    schedule=None,
    catchup=False,
) as dag:

    schema = {
        "id": "bigint",
        "data": "string",
        "category": "string"
    }

    data_source = DataSourceConfig(
        connection_id="iceberg_default",
        path="",
        table_name="iceberg_table",
        format_type=FormatType.ICEBERG.value,
        db_name="test",
        table_schema=schema,
        prompts=["Show a sample of 10 records from the table",
                 "List all unique category in the table",
                 ],
        sql_queries=[]
    )

    llm_query_generator = LLMQueryGeneratorOperator(
        task_id="LLMGenerateSQLQueries",
        data_sources=[data_source],
    )

    approve_llm_op = ApprovalOperator(
        task_id="ApproveLLMOutputQueries",
        subject="Are the following queries appropriate for the given prompts?",
        body="""{{ ti.xcom_pull(task_ids='LLMGenerateSQLQueries')['view'] }}""",
        defaults="Reject",
        execution_timeout=datetime.timedelta(minutes=1),
    )

    analytics_op = AnalyticsOperator(
        task_id="AnalyticsOp",
        data_source_configs="""{{ ti.xcom_pull(task_ids='LLMGenerateSQLQueries')['input'] }}""",
    )

    validate_results = ApprovalOperator(
        task_id="ValidateResults",
        subject="Are the following results correct?",
        body="""{{ ti.xcom_pull(task_ids='AnalyticsOp') }}""",
        defaults="Reject",
        execution_timeout=datetime.timedelta(minutes=1),
    )

    llm_query_generator >> approve_llm_op >> analytics_op >> validate_results
