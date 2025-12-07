import pandas as pd
from google.cloud import storage
import gspread
from oauth2client.service_account import ServiceAccountCredentials

def extract_data_from_google_sheets(sheet_id, sheet_name, creds_json):
    # Scopes: Sheets + Drive
    scope = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly"
    ]

    # Authenticate the service account
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_json, scope)
    client = gspread.authorize(creds)

    # Open the Google Sheet using Sheet ID (correct)
    sheet = client.open_by_key(sheet_id).worksheet(sheet_name)

    # Read all data from the sheet
    data = sheet.get_all_records()

    # Convert to DataFrame
    df = pd.DataFrame(data)
    return df


def load_data_to_gcs(df, bucket_name, destination_blob_name, creds_json):
    # Initialize a GCS client
    storage_client = storage.Client.from_service_account_json(creds_json)

    # Access bucket
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    # Convert df to CSV in memory
    csv_data = df.to_csv(index=False)

    # Upload to GCS
    blob.upload_from_string(csv_data, content_type="text/csv")

    print(f"Data uploaded to gs://{bucket_name}/{destination_blob_name}")


if __name__ == "__main__":

    # === PARAMETERS ===
    SHEET_ID = "1JCCvQmtbRxdrfPrAbm-F9Y8WJofkxz1-yj_zNh_yS4U"    # ONLY SHEET ID
    SHEET_NAME = "interviews1"

    BUCKET_NAME = "lvc-tc-mn-d-bckt" 
    DESTINATION_BLOB_NAME = "interviews_scheduled_folder/raw/interviews_data_yyyymmdd_hhmmss.csv"

    CREDS_JSON = r"C:\lct-dm-p\lct-mn-auto\gcp-source\secrets\service_account_credentials.json"

    # Extract data
    df = extract_data_from_google_sheets(SHEET_ID, SHEET_NAME, CREDS_JSON)

    # Load to GCS
    load_data_to_gcs(df, BUCKET_NAME, DESTINATION_BLOB_NAME, CREDS_JSON)
