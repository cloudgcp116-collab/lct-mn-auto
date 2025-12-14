from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.exceptions import AirflowFailException

class GCSFileCountOperator(BaseOperator):
    """
    Counts files in a given GCS bucket + prefix.
    Fails if no files are found (production-ready check).
    """

    def __init__(
        self,
        bucket_name: str,
        prefix: str,
        gcp_conn_id: str = "google_cloud_default",
        **kwargs
    ):
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context):
        self.log.info(
            "Checking files in bucket=%s, prefix=%s",
            self.bucket_name,
            self.prefix
        )

        hook = GCSHook(gcp_conn_id=self.gcp_conn_id)

        files = hook.list(
            bucket_name=self.bucket_name,
            prefix=self.prefix
        )

        file_count = len(files)
        self.log.info("Total files found: %s", file_count)

        if file_count == 0:
            raise AirflowFailException("No files found in GCS path")

        # XCom output
        return file_count
