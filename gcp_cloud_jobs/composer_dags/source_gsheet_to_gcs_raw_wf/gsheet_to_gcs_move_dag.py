# -----------------------------
# DAG imports Sections - Sec1
# -----------------------------
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from google.cloud import storage 
from airflow.operators.empty import EmptyOperator 



# -----------------------------
# DAG VARIABLES - Sec2
# -----------------------------
sheet_id = "1JCCvQmtbRxdrfPrAbm-F9Y8WJofkxz1-yj_zNh_yS4U"
sheet_name = "interviews1"
creds_json = "/home/airflow/gcs/dags/creds/service_account_credentials.json"
bucket_name = "lvc-tc-mn-d-bckt" 
# i need to calculate the datetime part dynamically to have unique file names
current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
destination_blob_name = f"interviews_scheduled_folder/raw/interviews_data_{current_datetime}.csv"


# -----------------------------
# default_args - Sec3
# -----------------------------
default_args = {
    "owner": "data_engineering_team",
    "depends_on_past": False,
    "email": ["alerts@company.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}


# -----------------------------
# Python functions - Sec4
# -----------------------------
def greet_user(name, age, country):
    print(f"Hello {name}, age {age}, from {country}!")
    return "Function executed successfully"


def extract_data_from_google_sheets(sheet_id, sheet_name, creds_json):

    scope = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly"
    ]

    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_json, scope)
    client = gspread.authorize(creds)

    sheet = client.open_by_key(sheet_id).worksheet(sheet_name)

    data = sheet.get_all_records()
    df = pd.DataFrame(data)

    print(df.head())  # so logs show results
    storage_client = storage.Client.from_service_account_json(creds_json)

    # Access bucket
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    # Convert df to CSV in memory
    csv_data = df.to_csv(index=False)

    # Upload to GCS
    blob.upload_from_string(csv_data, content_type="text/csv")

    print(f"Data uploaded to gs://{bucket_name}/{destination_blob_name}")





# --------------------------------
# AIRFLOW DAG INSTANTIATION - Sec5
# --------------------------------
with DAG(
    dag_id="source_gsheet_to_gcs_raw_python_operator_dag_v4",
    description="Demo DAG using PythonOperator with parameters",
    default_args=default_args,
    start_date=datetime(2025, 12, 5),
    schedule_interval="0 0 * * *", # what is the schedule interval for daily at midnight
    catchup=False,
    tags=["prod", "source_gsheet_to_gcs_raw"]
):
    # give a task using emty operator
    start = EmptyOperator(task_id="start")  
    # -----------------------------
    # PythonOperator Task1
    # -----------------------------
    greet_task = PythonOperator(
        task_id="greet_user_task",
        python_callable=greet_user,
        op_kwargs={
            "name": "Lavu'sCloudTech",
            "age": 2,
            "country": "India"
        },
        depends_on_past=False,
        retries=3,
        retry_delay=timedelta(minutes=2)
    )

    # -----------------------------
    # PythonOperator Task2
    # -----------------------------
    read_sheet_and_load_task = PythonOperator(
        task_id="read_sheet_and_load",
        python_callable=extract_data_from_google_sheets,
        op_kwargs={
            "sheet_id": sheet_id,
            "sheet_name": sheet_name,
            "creds_json": creds_json
        }
    )
    # give a task using emty operator
    end = EmptyOperator(task_id="end")

    # -----------------------------
    # TASK DEPENDENCIES - Sec7
    # -----------------------------
    start >> greet_task >> read_sheet_and_load_task >> end
