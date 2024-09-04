import psycopg2
import json
import io
import os
from minio import Minio
from decimal import Decimal
import datetime
from dotenv import load_dotenv

load_dotenv()

# Extract data from PostgreSQL
def extract_data():
    db_connection = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        dbname=os.getenv("DB_NAME")
    )
    cursor = db_connection.cursor()
    cursor.execute("SELECT * FROM products")
    rows = cursor.fetchall()

    # Get column names from cursor
    column_names = [desc[0] for desc in cursor.description]

    # Convert rows into a list of dictionaries
    result = [dict(zip(column_names, row)) for row in rows]
    
    cursor.close()
    db_connection.close()
    return result

# Convert data to JSON
def transform_data(rows):
    # Custom encoder to handle Decimal and datetime types
    def json_serializer(obj):
        if isinstance(obj, Decimal):
            return float(obj)  # or str(obj) if you prefer
        elif isinstance(obj, (datetime.datetime, datetime.date)):
            return obj.isoformat()  # Convert datetime to string
        # Add more type checks here if necessary
        raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")
    
    return json.dumps(rows, default=json_serializer)

# Load JSON data to Minio
def load_data_to_minio(json_data):
    minio_client = Minio(
       os.getenv("MINIO_ENDPOINT"),
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=False
    )
    bucket_name = "my-bucket"
    file_name = "data.json"

    # Ensure bucket exists
    if not minio_client.bucket_exists(bucket_name):
        print(f"Creating bucket: {bucket_name}")
        minio_client.make_bucket(bucket_name)

    print(f"Uploading data to bucket: {bucket_name} as {file_name}")
    
    # Upload the JSON data
    minio_client.put_object(
        bucket_name,
        file_name,
        data=io.BytesIO(json_data.encode('utf-8')),
        length=len(json_data),
        content_type="application/json"
    )
    print("Upload complete")

# Main function to run the entire process
def sync_db_to_minio():
    try:
        rows = extract_data()
        json_data = transform_data(rows)
        load_data_to_minio(json_data)
        print("Data sync to Minio was successful")
    except Exception as e:
        print(f"Error during sync: {str(e)}")
