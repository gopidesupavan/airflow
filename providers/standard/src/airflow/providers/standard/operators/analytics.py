from functools import cached_property

from datafusion import SessionContext

from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook
from airflow.sdk import BaseOperator
from airflow.sdk import BaseHook
from datafusion.object_store import AmazonS3




class AnalyticsOperator(BaseOperator):
    """
    An operator that performs a simple analytics task.

    :param data: The data to be analyzed.
    """
    template_fields = ["query"]

    def __init__(self,
                 query: str,
                 conn_id: str = "analytics_default",
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self.query = query
        self.conn_id = conn_id

    def execute(self, context):
        conn = BaseHook.get_connection(self.conn_id)
        self.log.info("Using connection: %s", conn.conn_type)
        self.log.info("Executing query: %s", self.query)
        ctx = SessionContext()
        if conn.conn_type == 'aws':
            g_hook = AwsGenericHook(aws_conn_id=self.conn_id)
            creds = g_hook.get_credentials()
            bucket = "016963197464-test"
            store = AmazonS3(
                    bucket_name=bucket,
                    region=g_hook.region_name,
                    access_key_id=creds.access_key,
                    secret_access_key=creds.secret_key,
                )

            ctx.register_object_store("s3://", store, None)
            ctx.register_parquet("trips", f"s3://{bucket}/data/")

        ctx.table("trips").show()
        result = ctx.sql(f"""{self.query}""").to_pydict()
        return result

    def _register_with_datafusion(self, scheme: str, store) -> None:
        self._datafusion_ctx.register_object_store("s3://", store, None)
        self._datafusion_ctx.register_parquet("trips", "s3://213833969848-test/data/")

    @cached_property
    def _datafusion_ctx(self):
        ctx = SessionContext()
        return ctx

