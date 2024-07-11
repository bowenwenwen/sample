from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '../scripts'))

from fetch_data import fetch_data, save_data
from process_data import process_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'api_data_pipeline',
    default_args=default_args,
    description='A pipeline to fetch and process API data',
    schedule_interval='@daily',
)

def fetch_data_task(**kwargs):
    api_url = "https://api.example.com/data"
    data = fetch_data(api_url)
    save_data(data, "/path/to/save/data.json")

def process_data_task(**kwargs):
    file_path = "/path/to/save/data.json"
    redshift_table = "public.processed_data"
    redshift_credentials = {
        "url": "jdbc:redshift://your-redshift-cluster:5439/your-database",
        "user": "your-username",
        "password": "your-password"
    }
    process_data(file_path, redshift_table, redshift_credentials)

fetch_task = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_data_task,
    dag=dag,
)

process_task = PythonOperator(
    task_id='process_data',
    python_callable=process_data_task,
    dag=dag,
)

fetch_task >> process_task
