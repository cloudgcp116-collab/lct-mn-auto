############################################
# IMPORTS
############################################

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    initcap,
    lower,
    to_date,
    date_format,
    trim
)
from google.cloud import storage
import re


############################################
# SPARK SESSION
############################################

spark = SparkSession.builder \
    .master("yarn") \
    .appName("gcs_to_bq_interviews_job") \
    .getOrCreate()

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

############################################
# GCS CONFIGURATION
############################################

PROJECT_ID = "lvc-tc-mn-d"
BUCKET_NAME = "lvc-tc-mn-d-bckt"
PREFIX_PATH = "interviews_scheduled_folder/raw/"

storage_client = storage.Client(project=PROJECT_ID)
bucket = storage_client.bucket(BUCKET_NAME)
blobs = bucket.list_blobs(prefix=PREFIX_PATH)

############################################
# FILE NAME VALIDATION
############################################

def is_valid_filename(filename: str) -> bool:
    """
    Valid file pattern:
    interviews_data_YYYYMMDD_HHMMSS.csv
    """
    pattern = r"^interviews_data_\d{8}_\d{6}\.csv$"
    return bool(re.match(pattern, filename))

############################################
# DATA TRANSFORMATIONS
############################################

def transform_interviews_df(df):
    """
    Cleans column names, applies business rules,
    and prepares data for BigQuery
    """

    # ----------------------------------------
    # 1. TRIM COLUMN NAMES & VALUES
    # ----------------------------------------
    df = df.select([trim(col(c)).alias(c.strip()) for c in df.columns])

    # ----------------------------------------
    # 2. BUSINESS TRANSFORMATIONS
    # ----------------------------------------
    transformed_df = df.select("*",

        to_timestamp(
            col("Timestamp"),
            "dd/MM/yyyy HH:mm:ss"
        ).alias("inserted_ts"),

        initcap(
            col("Name")
        ).alias("interview_candidate_name"), 

        to_date(
            col("Interview Date"),
            "dd/MM/yyyy"
        ).alias("interview_date"),

        date_format(
            col("Interview Time"),
            "HH:mm:ss"
        ).alias("interview_time"),

        initcap(
            col("Company Name")
        ).alias("company_name")
    )

    return transformed_df

############################################
# MAIN EXECUTION
############################################

for blob in blobs:

    filename = blob.name.split("/")[-1]
    print(f"📄 Processing file: {filename}")

    if not is_valid_filename(filename):
        print("❌ Invalid filename. Skipping.")
        continue

    print("✅ Valid filename detected")

    file_path = f"gs://{BUCKET_NAME}/{blob.name}"
    print(f"📥 Reading from: {file_path}")

    # ----------------------------------------
    # READ CSV FROM GCS
    # ----------------------------------------
    source_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(file_path)

    # ----------------------------------------
    # TRANSFORM DATA
    # ----------------------------------------
    final_df = transform_interviews_df(source_df)

    final_df.printSchema()
    final_df.show(5,truncate=False)

    # ----------------------------------------
    # WRITE TO BIGQUERY
    # ----------------------------------------
    final_df.write \
      .format("bigquery") \
      .option("table","lvc-tc-mn-d.interviews_stage.interviews_schedule") \
      .option("temporaryGcsBucket","gs://lvc-tc-mn-d-bckt/temp") \
      .mode("overwrite") \
      .save()


    print("✅ Data successfully written to BigQuery")

############################################
# JOB COMPLETE
############################################

print("🎉 GCS → BigQuery Spark Job Completed Successfully")
