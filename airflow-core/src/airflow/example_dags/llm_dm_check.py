from airflow.sdk import DAG, task
from airflow.providers.standard.llm.base import LLMDataQualityOperator
from airflow.providers.standard.operators.analytics import AnalyticsOperator
from airflow.providers.standard.operators.hitl import ApprovalOperator
from airflow.providers.standard.datafusion.config import DataSourceConfig, FormatType
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import datetime

from airflow.sdk import DAG, Asset, AssetWatcher, chain

from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, Asset, AssetWatcher

downstream_source = DataSourceConfig(
        connection_id="aws_default",
        path="s3://016963197464-test/",
        table_name="user_data",
        format_type=FormatType.PARQUET.value,
        db_name=None,
        table_schema={},
        prompts=[],
        sql_queries=[]
    )

with DAG(
    dag_id="llm_dq_check_s3_feed",
    start_date=None,
    schedule=None,
    catchup=False,
) as dag:

    llm_schema_check = LLMDataQualityOperator(
        task_id="CustomerDataS3Feed",
        data_sources=[downstream_source],
        prompt = """Generate data quality checks for the following requirements:

            1. Find count of records where id is null, it should be 0 otherwise fail
            2. Find count of records where age is less than 20, it should be 0 otherwise fail
        """
    )

    dq_check_result = ApprovalOperator(
        task_id="ApproveDQResults",
        subject="Are the data quality results acceptable?",
        body="""{{ ti.xcom_pull(task_ids='CustomerDataS3Feed')['view'] }}""",
        defaults="Reject",
        execution_timeout=datetime.timedelta(minutes=1),
    )

    llm_schema_check >> dq_check_result
