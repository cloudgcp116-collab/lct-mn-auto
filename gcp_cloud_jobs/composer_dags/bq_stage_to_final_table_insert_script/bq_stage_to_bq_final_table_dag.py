# i need a dag to run an insert script from bq staging table to bq final table
# use just bigqueryjobinert operartor
from google.cloud import bigquery
from airflow import models
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime, timedelta
from airflow.utils import trigger_rule

# STEP 3: Default DAG arguments
default_dag_args = {
    'start_date': datetime(2025, 12, 21),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
# STEP 4: Define DAG
with models.DAG(
    dag_id='bq_stage_to_bq_final_table_dag',
    description='DAG for inserting data from BQ staging table to BQ final table',
    schedule_interval="0 2 * * *", # daily at 2am
    default_args=default_dag_args,
    catchup=False,
) as dag:

    start = EmptyOperator(
        task_id='start'
    )

    bq_insert_job = BigQueryInsertJobOperator(
        task_id='bq_stage_to_final_table_insert',
        configuration={
            "query": {
                "query": """
                    INSERT INTO lvc-tc-mn-d.interviews_curated.interview_schedule (
    Timestamp,
    Name,
    Email,
    Contact Number,
    Interview Date,
    Interview Time,
    Interview Duration,
    Company Name,
    Interview Details,
    Email Address,
    inserted_ts,
    interview_candidate_name,
    interview_date,
    interview_time,
    company_name,
    record_date
)
SELECT
    b.timestamp,
    b.Name,
    b.Email,
    b.Contact Number,
    b.Interview Date,
    b.Interview Time,
    b.Interview Duration,
    b.Company Name,
    b.Interview Details,
    b.Email Address,
    b.inserted_ts,
    b.interview_candidate_name,
    b.interview_date,
    b.interview_time,
    b.company_name, 
    b.record_date
FROM lvc-tc-mn-d.interviews_stage.interviews_schedule b;
                """,
                "useLegacySql": False,
            }
        },
    )

    end = EmptyOperator(
        task_id='end',
        trigger_rule=trigger_rule.TriggerRule.ALL_DONE
    )

    start >> bq_insert_job >> end