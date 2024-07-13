from airflow.providers.amazon.aws.hooks.kinesis_analytics import KinesisAnalyticsV2Hook


class TestKinesisAnalyticsV2CustomWaiters:
    def test_service_waiters(self):
        assert "application_operation_complete" in KinesisAnalyticsV2Hook().list_waiters()
