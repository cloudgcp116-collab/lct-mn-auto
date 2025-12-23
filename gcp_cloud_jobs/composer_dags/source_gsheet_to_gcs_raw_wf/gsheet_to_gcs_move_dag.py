# -----------------------------
# DAG imports - Sec1
# -----------------------------
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.operators.email import EmailOperator
from airflow.utils.trigger_rule import TriggerRule

import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from google.cloud import storage


# -----------------------------
# DAG VARIABLES - Sec2
# -----------------------------
sheet_id = "1JCCvQmtbRxdrfPrAbm-F9Y8WJofkxz1-yj_zNh_yS4U"
sheet_name = "interviews1"
creds_json = "/home/airflow/gcs/dags/creds/service_account_credentials.json"
bucket_name = "lvc-tc-mn-d-bckt"

EMAIL_DL = ["lavu2018hani@gmail.com"]


# -----------------------------
# default_args - Sec3
# -----------------------------
default_args = {
    "owner": "data_engineering_team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# -----------------------------
# PYTHON FUNCTIONS - Sec4
# -----------------------------
def greet_user(name, age, country):
    print(f"Hello {name}, age {age}, from {country}!")


def extract_data_from_google_sheets(sheet_id, sheet_name, creds_json):

    scope = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]

    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_json, scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(sheet_id).worksheet(sheet_name)

    df = pd.DataFrame(sheet.get_all_records())

    if df.empty:
        print("No data found in Google Sheet")
        return

    # Parse timestamp
    df["timestamp"] = pd.to_datetime(
        df["timestamp"], format="%m/%d/%Y %H:%M:%S", errors="coerce"
    )
    df["record_date"] = df["timestamp"].dt.date

    # Full / Incremental logic
    try:
        last_load_date = Variable.get("last_load_date")
        last_load_date = datetime.strptime(last_load_date, "%Y-%m-%d").date()
        df = df[df["record_date"] > last_load_date]
        load_type = "INCREMENTAL LOAD"
    except Exception:
        load_type = "FULL LOAD"

    print(f"Load Type: {load_type}")
    print(f"Records to load: {len(df)}")

    if df.empty:
        print("No new records to load")
        return

    # Upload to GCS
    current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
    destination_blob_name = (
        f"interviews_scheduled_folder/raw/interviews_data_{current_datetime}.csv"
    )

    storage_client = storage.Client.from_service_account_json(creds_json)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(df.to_csv(index=False), content_type="text/csv")

    print(f"Uploaded to gs://{bucket_name}/{destination_blob_name}")

    # Update last_load_date
    today = datetime.today().strftime("%Y-%m-%d")
    Variable.set("last_load_date", today)


# --------------------------------
# DAG DEFINITION - Sec5
# --------------------------------
with DAG(
    dag_id="gsheet_to_gcs_raw_data_pipe_dag",
    description="Google Sheet to GCS with full & incremental load + email alerts",
    default_args=default_args,
    start_date=datetime(2025, 12, 5),
    schedule_interval="0 0 * * *",
    catchup=False,
    tags=["prod", "source_gsheet_to_gcs_raw"],
) as dag:

    start = EmptyOperator(task_id="start")

    greet_task = PythonOperator(
        task_id="greet_user_task",
        python_callable=greet_user,
        op_kwargs={
            "name": "Lavu'sCloudTech",
            "age": 2,
            "country": "India",
        },
    )

    read_sheet_and_load_task = PythonOperator(
        task_id="read_sheet_and_load",
        python_callable=extract_data_from_google_sheets,
        op_kwargs={
            "sheet_id": sheet_id,
            "sheet_name": sheet_name,
            "creds_json": creds_json,
        },
    )

    # -----------------------------
    # SUCCESS EMAIL
    # -----------------------------
    success_email = EmailOperator(
        task_id="success_email",
        to=EMAIL_DL,
        subject="✅ SUCCESS: gsheet_to_gcs_raw_data_pipeline_dag",
        html_content="""
        <h3>DAG Completed Successfully 🎉</h3>
        <p>DAG: gsheet_to_gcs_raw_data_pipeline_dag</p>
        <p>Execution Time: {{ ts }}</p>
        """,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # -----------------------------
    # FAILURE EMAIL
    # -----------------------------
    failure_email = EmailOperator(
        task_id="failure_email",
        to=EMAIL_DL,
        subject="❌ FAILURE: gsheet_to_gcs_raw_data_pipeline_dag",
        html_content="""
        <h3>DAG Failed 🚨</h3>
        <p>DAG: gsheet_to_gcs_raw_data_pipeline_dag</p>
        <p>Execution Time: {{ ts }}</p>
        <p>Please check Airflow logs.</p>
        """,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    end = EmptyOperator(task_id="end")

    # -----------------------------
    # DEPENDENCIES
    # -----------------------------
  
    start >> greet_task >> read_sheet_and_load_task
    read_sheet_and_load_task >> [success_email, failure_email] >> end