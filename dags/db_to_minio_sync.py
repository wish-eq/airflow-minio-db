import sys
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# # Get scripts path from the .env file and add it to sys.path
# scripts_path = os.getenv('SCRIPTS_PATH')
# if scripts_path:
#     sys.path.append(scripts_path)
# else:
#     raise ValueError("SCRIPTS_PATH not found in the environment variables")
sys.path.append('/Users/wishmarukaptak/internship/TCC/POC/airflow-minio-db/scripts')

from db_minio_sync import sync_db_to_minio
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Define the DAG
default_args = {
    'owner': 'Icque',
    'start_date': datetime(2024, 9, 4),
}

dag = DAG(
    'db_to_minio_sync',
    default_args=default_args,
    description='Sync DB data to Minio as JSON',
    schedule_interval='@daily',
    catchup=False
)

# Define the task
sync_task = PythonOperator(
    task_id='sync_db_to_minio',
    python_callable=sync_db_to_minio,
    dag=dag
)

sync_task
