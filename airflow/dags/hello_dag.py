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
from fetch_api import fetch_api  # your script must have fetch_api() function
from upload_files import upload_files
from preprocess_and_upload import preprocess_and_upload
from loadingintopostgres import loadingintopostgres


# -----------------------------
# Define DAG
# -----------------------------
with DAG(
    dag_id="pipeline_process3",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",  # run daily; set None for manual runs
    catchup=False,
    tags=["api", "fetch"],
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_api",
        python_callable=fetch_api,
    )

    upload_task = PythonOperator(
        task_id="upload_minio",
        python_callable=upload_files,
    )
 
    preprocess_task = PythonOperator(
        task_id="preprocess",
        python_callable=preprocess_and_upload,
    )

    loadintopostgresql= PythonOperator(
        task_id="load_postgres",
        python_callable=loadingintopostgres,
    )


    fetch_task >> upload_task >> preprocess_task >> loadintopostgresql
