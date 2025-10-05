from airflow.sdk import DAG, task
from airflow.providers.standard.operators.llm import LLMQueryGeneratorOperator
from airflow.providers.standard.llm.base import LLMSchemaCompareOperator
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
        path="s3://431797214645-test/",
        table_name="user_data",
        format_type=FormatType.PARQUET.value,
        db_name=None,
        table_schema={},
        prompts=[],
        sql_queries=[]
    )

upstream_source = DataSourceConfig(
    connection_id="pg_default",
    path="",
    table_name="employees",
    format_type=FormatType.POSTGRES.value,
    db_name="customer",
    table_schema={},
    prompts=[],
    sql_queries=[]
)

# trigger = MessageQueueTrigger(scheme="sqs", sqs_queue="https://sqs.us-east-1.amazonaws.com/735234585484/customer-data-feed", region_name="us-east-1")
# asset = Asset("customer_s3_feed_asset", watchers=[AssetWatcher(name="sqs_watcher", trigger=trigger)])

with DAG(
    dag_id="llm_schema_check_s3_feed",
    start_date=None,
    schedule=None,
    catchup=False,
) as dag:

    llm_schema_check = LLMSchemaCompareOperator(
        task_id="CustomerDataS3Feed",
        data_sources=[downstream_source, upstream_source],
        prompt = """Please analyze:
            1. Identify any column addition or removal breaking changes between the downstream and upstream schemas
            2. Generate migration queries for the breaking changes for upstream table
        """
    )

    schema_check_result = ApprovalOperator(
        task_id="ApproveSchemaChanges",
        subject="Are the schema changes acceptable?",
        body="""{{ ti.xcom_pull(task_ids='CustomerDataS3Feed')['view'] }}""",
        defaults="Reject",
        execution_timeout=datetime.timedelta(minutes=1),
    )

    sql_execute = SQLExecuteQueryOperator(
        task_id="ExecuteMigrationQueries",
        sql="""{{ ti.xcom_pull(task_ids='CustomerDataS3Feed')['migration_queries'] }}""",
        conn_id="pg_default",
    )

    llm_schema_check >> schema_check_result >> sql_execute
