from airflow import DAG
from datetime import datetime
from custom_operators.gcs_file_count_operator import GCSFileCountOperator
from airflow.operators.python import PythonOperator

def print_file_count(**context):
    count = context["ti"].xcom_pull(task_ids="count_gcs_files")
    print(f"File count from GCS: {count}")

with DAG(
    dag_id="gcs_custom_operator_demo",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["gcs", "custom-operator"]
) as dag:

    count_files = GCSFileCountOperator(
        task_id="count_gcs_files",
        bucket_name="lvc-tc-mn-d-bckt",
        prefix="interviews_scheduled_folder/raw"
    )

    show_count = PythonOperator(
        task_id="print_file_count",
        python_callable=print_file_count
    )

    count_files >> show_count
