import boto3
import pandas as pd
import psycopg2
from io import BytesIO
import re

# -----------------------------
# MinIO Config
# -----------------------------
BUCKET_NAME = "test"
MINIO_ENDPOINT = "http://minio:9000"  # Docker service name
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"

# -----------------------------
# PostgreSQL Config (app DB)
# -----------------------------
DB_HOST = "app-db"     # Docker service name
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASS = "postgresql"

# -----------------------------
# Unified Function: Load Latest CSV from MinIO and insert into PostgreSQL
# -----------------------------
def loadingintopostgres():
    print("Connecting to MinIO...")
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY
    )

    # Step 1: Find the latest date folder in 'processed/'
    result = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix="processed/")
    if "Contents" not in result:
        raise ValueError("No files found in MinIO under 'processed/'")

    all_keys = [obj['Key'] for obj in result['Contents']]
    date_folders = [re.search(r'processed/(\d{4}-\d{2}-\d{2})/', key).group(1)
                    for key in all_keys if re.search(r'processed/(\d{4}-\d{2}-\d{2})/', key)]

    if not date_folders:
        raise ValueError("No date folders found in MinIO 'processed/' path")

    latest_date = max(date_folders)
    print(f"Latest date folder found: {latest_date}")

    # Step 2: Get the first CSV file inside the latest folder
    prefix_path = f"processed/{latest_date}/"
    csv_files = [key for key in all_keys if key.startswith(prefix_path) and key.endswith(".csv")]

    if not csv_files:
        raise ValueError(f"No CSV files found in {prefix_path}")

    latest_csv = csv_files[0]
    print(f"CSV file found: {latest_csv}")

    obj = s3.get_object(Bucket=BUCKET_NAME, Key=latest_csv)
    df = pd.read_csv(BytesIO(obj['Body'].read()))
    print(f"CSV loaded from MinIO, {len(df)} rows found.")

    # Step 3: Load Data into PostgreSQL (star schema)
    conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
    cur = conn.cursor()

    # Create tables
    cur.execute("""
        CREATE TABLE IF NOT EXISTS category1 (
            id SERIAL PRIMARY KEY,
            category_name TEXT UNIQUE
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS product1 (
            id INT PRIMARY KEY,
            title TEXT,
            category_id INT REFERENCES category1(id)
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS product_ratings1 (
            product_id INT REFERENCES product1(id),
            price NUMERIC,
            rating_rate NUMERIC,
            rating_count INT,
            PRIMARY KEY (product_id)
        );
    """)
    conn.commit()

    # Insert categories
    categories = df['category'].unique().tolist()
    cur.execute("""
        INSERT INTO category1(category_name)
        SELECT UNNEST(%s::text[])
        ON CONFLICT (category_name) DO NOTHING;
    """, (categories,))
    conn.commit()

    # Fetch category mapping
    cur.execute("SELECT id, category_name FROM category1;")
    category_mapping = {name: cid for cid, name in cur.fetchall()}

    # Insert products
    for _, row in df.iterrows():
        cat_id = category_mapping[row['category']]
        cur.execute("""
            INSERT INTO product1(id, title, category_id)
            VALUES (%s, %s, %s)
            ON CONFLICT (id) DO NOTHING;
        """, (row['id'], row['title'], cat_id))
    conn.commit()

    # Insert product ratings
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO product_ratings1(product_id, price, rating_rate, rating_count)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (product_id) DO NOTHING;
        """, (row['id'], row['price'], row['rating_rate'], row['rating_count']))
    conn.commit()

    cur.close()
    conn.close()
    print("✅ Data loaded into PostgreSQL star schema successfully!")

# -----------------------------
# Main execution (for testing outside Airflow)
# -----------------------------
if __name__ == "__main__":
    loadingintopostgres()
