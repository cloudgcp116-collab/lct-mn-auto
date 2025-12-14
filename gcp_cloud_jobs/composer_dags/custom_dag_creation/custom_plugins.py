from airflow.plugins_manager import AirflowPlugin
from custom_operators.gcs_file_count_operator import GCSFileCountOperator

class CustomGCSPlugin(AirflowPlugin):
    name = "custom_gcs_plugin"

    operators = [
        GCSFileCountOperator
    ]
