from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import json
import pandas as pd
import smtplib
import gspread
import google_crc32c
from google.cloud import secretmanager
from google.cloud import bigquery
from google.oauth2.service_account import Credentials

# -------------------------------------------------
# CONFIG
# -------------------------------------------------
PROJECT_ID = "lvc-tc-mn-d"
SECRET_PROJECT_ID = "175579495168"
SECRET_ID = "interviews_secret_sa"

SHEET_ID = "18LJ0dh7W8u698vPsB6Ew6xOgDONPynYcYE0SU6UzbiU"
GIVERS_WORKSHEET = "Candidate_Availability"
TAKERS_WORKSHEET = "Panel_Availability"

DATASET_ID = "interviews_stage"
TABLE_ID = "scheduled_interviews"

# -------------------------------------------------
# SECRET MANAGER
# -------------------------------------------------
def get_secret(project_id, secret_id, version="latest"):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version}"
    response = client.access_secret_version(request={"name": name})

    crc32c = google_crc32c.Checksum()
    crc32c.update(response.payload.data)

    if response.payload.data_crc32c != int(crc32c.hexdigest(), 16):
        raise ValueError("Secret corrupted")

    return response.payload.data.decode("UTF-8")

# -------------------------------------------------
# GOOGLE SHEET → DATAFRAME
# -------------------------------------------------
def get_df_from_sheet(sa_json, sheet_id, worksheet):
    creds = Credentials.from_service_account_info(
        sa_json,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
    )

    client = gspread.authorize(creds)
    sheet = client.open_by_key(sheet_id)
    ws = sheet.worksheet(worksheet)

    df = pd.DataFrame(ws.get_all_records())
    df.columns = df.columns.str.strip().str.lower()
    return df

# -------------------------------------------------
# MAIN PIPELINE FUNCTION
# -------------------------------------------------
def interview_matching_pipeline():
    # ---- Get Service Account JSON
    sa_json = json.loads(get_secret(SECRET_PROJECT_ID, SECRET_ID))

    # ---- Read Sheets
    df_giver = get_df_from_sheet(sa_json, SHEET_ID, GIVERS_WORKSHEET)
    df_taker = get_df_from_sheet(sa_json, SHEET_ID, TAKERS_WORKSHEET)

    today = datetime.now().date()

    # ---- Date Cleaning
    for df in [df_giver, df_taker]:
        df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.date
        df.dropna(subset=['date'], inplace=True)

    df_giver = df_giver[df_giver['date'] == today]
    df_taker = df_taker[df_taker['date'] == today]

    # ---- Merge
    merged_df = pd.merge(
        df_giver,
        df_taker,
        on=['date', 'time'],
        how='inner',
        suffixes=('_giver', '_taker')
    )

    if merged_df.empty:
        print("No matching interviews today")
        return

    # -------------------------------------------------
    # EMAIL
    # -------------------------------------------------
    sender_email = get_secret(SECRET_PROJECT_ID, "SENDER_EMAIL")
    sender_password = get_secret(SECRET_PROJECT_ID, "SENDER_PASSWORD")

    server = smtplib.SMTP("smtp.gmail.com", 587)
    server.starttls()
    server.login(sender_email, sender_password)

    for _, row in merged_df.iterrows():
        subject = "Interview Slot Matched"
        body = f"""
Interview scheduled on {row['date']} at {row['time']}

GIVER: {row['name_giver']} | {row['email']}
TAKER: {row['name_taker']} | {row['email_id']}
"""
        message = f"Subject: {subject}\n\n{body}"

        server.sendmail(sender_email, row['email'], message)
        server.sendmail(sender_email, row['email_id'], message)

    server.quit()

    # -------------------------------------------------
    # BIGQUERY LOAD
    # -------------------------------------------------
    bq_client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    merged_df['date'] = pd.to_datetime(merged_df['date'])
    job = bq_client.load_table_from_dataframe(merged_df, table_ref)
    job.result()

    print("Pipeline executed successfully")

# -------------------------------------------------
# DAG DEFINITION
# -------------------------------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 12, 21),
    "retries": 1
}

with DAG(
    dag_id="interview_matching_dag_v2",
    default_args=default_args,
    schedule_interval=None, # "0 8 * * *",  # Daily at 8 AM
    catchup=False,
    tags=["google-sheets", "secret-manager", "bigquery", "email"]
) as dag:

    start_task = EmptyOperator(task_id="start")

    run_pipeline = PythonOperator(
        task_id="run_interview_matching_pipeline",
        python_callable=interview_matching_pipeline
    )

    end_task = EmptyOperator(task_id="end")

    start_task >>  run_pipeline >> end_task
