from airflow.models import BaseOperator
from airflow.providers.standard.triggers.sample_trigger import SampleTrigger



class SampleOperator(BaseOperator):
    """
    Sample operator that prints something.
    :param my_operator_param: A parameter that you can pass to this operator.
    :type my_operator_param: str
    """

    def __init__(self,**kwargs) -> None:
        super().__init__(**kwargs)

    def execute(self, context):
        self.log.info("In operator")

        self.defer(
            trigger=SampleTrigger(),
            method_name="execute_complete",
        )

    def execute_complete(self, context, event):
        self.log.info("Hello World! Complete", event)
