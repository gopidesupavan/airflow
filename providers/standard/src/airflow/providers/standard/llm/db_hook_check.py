from airflow.providers.common.sql.hooks.handlers import fetch_all_handler
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.sdk import BaseOperator, BaseHook


class ExecuteQuery(BaseOperator):
    """
    An operator that executes a SQL query and returns the results in a tabular format.

    :param sql_query: The SQL query to execute.
    """

    def __init__(self, sql_query: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.sql_query = sql_query

    def execute(self, context):
        connection = BaseHook.get_connection("my_db_connection")
        hook = connection.get_hook(hook_params={})
        result = hook.get_schema("users")
        print(result)
        print(hook.run("SELECT * FROM users", handler=fetch_all_handler))
