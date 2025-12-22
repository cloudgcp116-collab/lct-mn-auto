from datetime import datetime
from /home/airflow/gcs/dags/interviews_scheduled_folder/mock_interviews_scheduler import (
    read_and_filter_data,
    mergefiltered_data,
    send_email_notifications,
    insert_into_bigquery
)

# -------------------------------------------------
# CONFIG
# -------------------------------------------------
PROJECT_ID = "175579495168"
SECRET_ID = "interviews_secret_sa"

SHEET_ID = "18LJ0dh7W8u698vPsB6Ew6xOgDONPynYcYE0SU6UzbiU"
GIVERS_WORKSHEET = "Candidate_Availability"
TAKERS_WORKSHEET = "Panel_Availability"


def run_interview_matching():
    givers, takers = read_and_filter_data(
        PROJECT_ID,
        SECRET_ID,
        SHEET_ID,
        GIVERS_WORKSHEET,
        TAKERS_WORKSHEET
    )

    merged_df = mergefiltered_data(givers, takers)

    send_email_notifications(merged_df)
    insert_into_bigquery(merged_df)
