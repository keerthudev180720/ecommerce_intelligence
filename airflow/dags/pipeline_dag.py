from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

# Add scripts folder to PYTHONPATH (inside container)
sys.path.insert(0, "/opt/airflow/scripts")

# Import your pipeline functions
from fetch_api import fetch_api
from upload_files import upload_files
from preprocess_and_upload import preprocess_and_upload
from loadingintopostgres import loadingintopostgres

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ecommerce_pipeline",
    default_args=default_args,
    description="E-commerce price intelligence pipeline",
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),  # set to past so runs are scheduled
    catchup=False,
) as dag:

    task1 = PythonOperator(
        task_id="fetch_api",
        python_callable=fetch_api,
    )

    task2 = PythonOperator(
        task_id="upload_minio",
        python_callable=upload_files,
    )

    task3 = PythonOperator(
        task_id="preprocess",
        python_callable=preprocess_and_upload,
    )

    task4 = PythonOperator(
        task_id="load_postgres",
        python_callable=loadingintopostgres,
    )

    task1 >> task2 >> task3 >> task4
