import boto3
import os
from datetime import datetime

# -----------------------------
# Configurations
# -----------------------------
BUCKET_NAME = "test"  # MinIO bucket
LOCAL_RAW_DIR = "/opt/airflow/data/raw/all_products"  # Your existing folder
MINIO_ENDPOINT = "http://minio:9000" 
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"

# -----------------------------
# Upload Files to MinIO
# -----------------------------
def upload_files():
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY
    )

    # Ensure bucket exists
    try:
        s3.head_bucket(Bucket=BUCKET_NAME)
    except:
        print(f"Bucket {BUCKET_NAME} not found. Creating it...")
        s3.create_bucket(Bucket=BUCKET_NAME)

    # Upload all files from local raw directory
    for file in os.listdir(LOCAL_RAW_DIR):
        file_path = os.path.join(LOCAL_RAW_DIR, file)
        if os.path.isfile(file_path):
            date_folder = datetime.now().strftime("%Y-%m-%d")
            key = f"raw/{date_folder}/{file}"
            s3.upload_file(file_path, BUCKET_NAME, key)
            print(f"Uploaded {file} to MinIO: {BUCKET_NAME}/{key}")

# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    upload_files()
