import os
import json
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from minio import Minio
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import psycopg2

load_dotenv()

# Minio client setup
def get_minio_client():
    minio_endpoint = os.getenv('MINIO_ENDPOINT')
    minio_access_key = os.getenv('MINIO_ACCESS_KEY')
    minio_secret_key = os.getenv('MINIO_SECRET_KEY')
    
    if not minio_endpoint or not minio_access_key or not minio_secret_key:
        raise ValueError("MINIO environment variables are not set properly")

    minio_client = Minio(
        minio_endpoint,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=False
    )
    return minio_client

# Function to read JSON files from Minio and insert them into the database
def load_data_from_minio_to_db():
    minio_client = get_minio_client()
    
    # List the JSON files in your bucket
    objects = minio_client.list_objects("my-bucket", prefix="", recursive=True)

    # Get database connection details from .env
    db_host = os.getenv('DB_HOST')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_name = os.getenv('DB_NAME')

    if not db_host or not db_user or not db_password or not db_name:
        raise ValueError("PostgreSQL environment variables are not set properly")

    # Connect to PostgreSQL using psycopg2
    conn = psycopg2.connect(
        host=db_host,
        user=db_user,
        password=db_password,
        dbname=db_name
    )
    cursor = conn.cursor()

    # Iterate over objects and process each JSON file
    for obj in objects:
        file_data = minio_client.get_object("my-bucket", obj.object_name)
        data = json.loads(file_data.read())

        # Prepare the SQL insert query
        insert_query = """
        INSERT INTO users (user_id, username, email, password_hash, first_name, last_name, is_seller, created_at, updated_at)
        VALUES %s
        ON CONFLICT (user_id) DO NOTHING
        """

        # Map JSON fields to SQL fields
        values = [
            (
                item['user_id'],               # Maps to user_id
                item['username'],              # Maps to username
                item['email'],                 # Maps to email
                item['password_hash'],         # Maps to password_hash
                item['first_name'],            # Maps to first_name
                item['last_name'],             # Maps to last_name
                item['is_seller'],             # Maps to is_seller (boolean or integer)
                item['created_at'],            # Maps to created_at
                item['updated_at']             # Maps to updated_at
            ) for item in data
        ]

        # Insert data into the database
        execute_values(cursor, insert_query, values)

    # Commit the transaction after processing all objects
    conn.commit()

    # Close the cursor and connection after all operations
    cursor.close()
    conn.close()

# Define the default arguments for the DAG
default_args = {
    'owner': 'Icque',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 1),
    'email_on_failure': False,
    'email_on_retry': False,
}

# Define the DAG
with DAG(
    'minio_to_db_sync',
    default_args=default_args,
    schedule_interval='@daily',  # You can set the schedule interval here
    catchup=False,
) as dag:

    # Task to insert data from Minio to PostgreSQL
    move_data_task = PythonOperator(
        task_id='move_data_from_minio_to_db',
        python_callable=load_data_from_minio_to_db
    )

    move_data_task
