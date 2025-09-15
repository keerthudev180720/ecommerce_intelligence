import boto3
import pandas as pd
from io import BytesIO
from datetime import datetime

# -----------------------------
# Configurations
# -----------------------------
BUCKET_NAME = "test"
MINIO_ENDPOINT = "http://localhost:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"

PROCESSED_PARQUET_PREFIX = "processed/"
OUTPUT_PREFIX = "analytics/"

# -----------------------------
# MinIO Client
# -----------------------------
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY
)

# -----------------------------
# Get Latest Processed Parquet
# -----------------------------
def get_latest_parquet():
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=PROCESSED_PARQUET_PREFIX)
    if "Contents" not in response:
        raise FileNotFoundError("No processed files found in MinIO bucket")

    files = [f for f in response["Contents"] if f["Key"].endswith(".parquet")]
    files_sorted = sorted(files, key=lambda x: x["LastModified"], reverse=True)
    latest_file_key = files_sorted[0]["Key"]

    print(f"Found latest processed file: {latest_file_key}")

    # Download to memory
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=latest_file_key)
    df = pd.read_parquet(BytesIO(obj['Body'].read()))
    return df, latest_file_key

# -----------------------------
# Category-Level Metrics
# -----------------------------
def category_metrics(df):
    cat_df = df.groupby("category").agg(
        total_products=("id", "count"),
        avg_price=("price", "mean"),
        max_price=("price", "max"),
        min_price=("price", "min")
    ).reset_index()
    return cat_df

# -----------------------------
# Top-Rated Products
# -----------------------------
def top_rated_products(df, top_n=10, min_ratings=10):
    top_df = df[df['rating_count'] >= min_ratings].sort_values("rating_rate", ascending=False).head(top_n)
    return top_df

# -----------------------------
# Price Deviation from Category Average
# -----------------------------
def price_deviation(df):
    df['category_avg_price'] = df.groupby('category')['price'].transform('mean')
    df['price_diff'] = df['price'] - df['category_avg_price']
    return df.sort_values("price_diff", ascending=False)

# -----------------------------
# Save DataFrame to MinIO
# -----------------------------
def save_to_minio(df, file_name):
    date_folder = datetime.now().strftime("%Y-%m-%d")
    key = f"{OUTPUT_PREFIX}{date_folder}/{file_name}"

    # Save to CSV in memory
    csv_buffer = BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=csv_buffer.getvalue())
    print(f"Saved analytics file to MinIO: {BUCKET_NAME}/{key}")

# -----------------------------
# Main Execution
# -----------------------------
if __name__ == "__main__":
    df, processed_key = get_latest_parquet()

    # 1️⃣ Category metrics
    cat_metrics = category_metrics(df)
    save_to_minio(cat_metrics, "category_metrics.csv")

    # 2️⃣ Top-rated products
    top_products = top_rated_products(df)
    save_to_minio(top_products, "top_rated_products.csv")

    # 3️⃣ Price deviation
    price_dev = price_deviation(df)
    save_to_minio(price_dev, "price_deviation.csv")
