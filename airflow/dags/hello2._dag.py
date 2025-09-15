from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

# -----------------------------
# Make scripts folder importable
# -----------------------------
sys.path.insert(0, "/opt/airflow/scripts")

# Import the function from your script

from upload_files import upload_files


# -----------------------------
# Define DAG
# -----------------------------
with DAG(
    dag_id="upload_minio",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",  # run daily; set None for manual runs
    catchup=False,
    tags=["upload", "minio"],
) as dag:



    upload_task = PythonOperator(
        task_id="upload_minio",
        python_callable=upload_files,
    )
