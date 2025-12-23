# STEP 1: Import Libraries
from datetime import timedelta, datetime
from airflow import models
from airflow.operators.bash import BashOperator  # updated import
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.utils import trigger_rule

# STEP 2: Define a start date
yesterday = datetime(2025, 12, 21)

# STEP 3: Default DAG arguments
default_dag_args = {
    'start_date': datetime(2025, 10, 17),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# STEP 4: Define DAG
with models.DAG(
    dag_id='dataproc_cluster_create_dag',
    description='DAG for deploying a Dataproc Cluster using modern operators',
    schedule_interval="30 0 * * *", # 
    default_args=default_dag_args,
    catchup=False,
) as dag:

    # STEP 5: Operators

    # BashOperator to print date
    print_date = BashOperator(
        task_id='print_date',
        bash_command='date'
    )

    # DataprocCreateClusterOperator
    create_dataproc = DataprocCreateClusterOperator(
        task_id='create_dataproc',
        project_id=models.Variable.get('project_id'),
        cluster_name='dataproc-cluster-demo-{{ ds_nodash }}',
        region=models.Variable.get('dataproc_zone'),  # e.g., us-west1
        cluster_config={
            "master_config": {
                "num_instances": 1,
                "machine_type_uri": "n1-standard-2",
                "disk_config": {"boot_disk_size_gb": 50},
            },
            "worker_config": {
                "num_instances": 2,
                "machine_type_uri": "n1-standard-2",
                "disk_config": {"boot_disk_size_gb": 50},
            },
            "gce_cluster_config": {
                "internal_ip_only": False,
            },
        },
    )

    # Sleep 1 minute for manual verification
    sleep_process1 = BashOperator(
        task_id='sleep_process1',
        bash_command='sleep 60'
    )

    # DataprocSubmitJobOperator (replacement for DataProcPySparkOperator)
    submit_pyspark_job = DataprocSubmitJobOperator(
        task_id='submit_pyspark_job',
        project_id=models.Variable.get('project_id'),
        region=models.Variable.get('dataproc_zone'),
        job={
            "reference": {"project_id": models.Variable.get('project_id')},
            "placement": {
                "cluster_name": 'dataproc-cluster-demo-{{ ds_nodash }}',
            },
            "pyspark_job": {
                "main_python_file_uri": "gs://lvc-tc-mn-d-bckt/working_interviews_pyspark_script.py",
                "args": [],
            },
        },
    )

    # Sleep 1 minute after job completion
    sleep_process2 = BashOperator(
        task_id='sleep_process2',
        bash_command='sleep 60'
    )

    # DataprocDeleteClusterOperator
    delete_dataproc = DataprocDeleteClusterOperator(
        task_id='delete_dataproc',
        project_id=models.Variable.get('project_id'),
        cluster_name='dataproc-cluster-demo-{{ ds_nodash }}',
        region=models.Variable.get('dataproc_zone'),
        trigger_rule=trigger_rule.TriggerRule.ALL_DONE,
    )

    # STEP 6: DAG Dependencies
    print_date >> create_dataproc >> sleep_process1 >> submit_pyspark_job >> sleep_process2 >> delete_dataproc
