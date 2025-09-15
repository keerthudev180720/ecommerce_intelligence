import boto3
import pandas as pd
import json
import os
from datetime import datetime
from io import BytesIO, StringIO

# -----------------------------
# Configurations
# -----------------------------
BUCKET_NAME = "test"
MINIO_ENDPOINT = "http://minio:9000"   # use service name in docker-compose, not localhost
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"

# -----------------------------
# Unified Preprocess Function
# -----------------------------
def preprocess_and_upload():
    # Initialize MinIO client
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY
    )

    # -----------------------------
    # Step 1: Get Latest Raw File from MinIO
    # -----------------------------
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix="raw/")
    if "Contents" not in response:
        raise FileNotFoundError("No raw files found in MinIO bucket")

    # Pick latest by LastModified
    files = sorted(response["Contents"], key=lambda x: x["LastModified"], reverse=True)
    latest_file_key = files[0]["Key"]
    print(f"Found latest raw file: {latest_file_key}")

    # Download into memory
    raw_obj = s3.get_object(Bucket=BUCKET_NAME, Key=latest_file_key)
    data = json.loads(raw_obj["Body"].read().decode("utf-8"))

    # -----------------------------
    # Step 2: Preprocess JSON Data
    # -----------------------------
    df = pd.DataFrame(data)

    # Drop unnecessary columns
    df = df.drop(columns=["description", "image"], errors="ignore")

    # Flatten rating column
    if "rating" in df.columns:
        df["rating_rate"] = df["rating"].apply(lambda x: x.get("rate") if isinstance(x, dict) else None)
        df["rating_count"] = df["rating"].apply(lambda x: x.get("count") if isinstance(x, dict) else None)
        df = df.drop(columns=["rating"], errors="ignore")

    print(f"Preprocessed {len(df)} records, columns: {list(df.columns)}")

    # -----------------------------
    # Step 3: Save Processed Data Back to MinIO (in-memory)
    # -----------------------------
    date_folder = datetime.now().strftime("%Y-%m-%d")
    base_name = os.path.basename(latest_file_key).replace(".json", "")
    csv_key = f"processed/{date_folder}/{base_name}.csv"
    parquet_key = f"processed/{date_folder}/{base_name}.parquet"

    # CSV → memory → upload
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=BUCKET_NAME, Key=csv_key, Body=csv_buffer.getvalue())

    # Parquet → memory → upload
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)
    s3.put_object(Bucket=BUCKET_NAME, Key=parquet_key, Body=parquet_buffer.getvalue())

    print(f"Uploaded processed CSV to MinIO: {BUCKET_NAME}/{csv_key}")
    print(f"Uploaded processed Parquet to MinIO: {BUCKET_NAME}/{parquet_key}")

    return df, csv_key, parquet_key

# -----------------------------
# Main Execution
# -----------------------------
if __name__ == "__main__":
    df, csv_key, parquet_key = preprocess_and_upload()
