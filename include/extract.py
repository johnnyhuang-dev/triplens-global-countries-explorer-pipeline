import requests
from dotenv import load_dotenv
import os
import json
import io
from minio import Minio
from minio.error import S3Error

load_dotenv()

API_KEY=os.getenv("API_KEY")
MINIO_ACCOUNT=os.getenv("MINIO_ACCOUNT")
MINIO_KEY=os.getenv("MINIO_KEY")
BASE_URL='https://api.restcountries.com/countries/v5'

def request_api():
    """Short function to request API data and create a new file with the extracted data"""
    try: 
        # Request API access for countries data
        response = requests.get(
        f'{BASE_URL}?q=canada',
        headers={'Authorization': f'Bearer {API_KEY}'}
        )

        # Upload raw countries data from API to MinIO bucket in JSON format
        if response.status_code == 200:
            data = response.json()
            print("Successfully added JSON data into new json file")

        else:
            print(f"API Error Response: {response.text}")

    except Exception as e:
        print(e)

    return data

def upload():

    data = request_api()

    data = json.dumps(data, ensure_ascii=False).encode("utf-8")

    # Wrap data bytes in BytesIO to give it a .read() method
    data_stream = io.BytesIO(data)

    # Initialize the client
    client = Minio(
        endpoint="localhost:9000",       
        access_key=MINIO_ACCOUNT,    
        secret_key=MINIO_KEY,    
        secure=False 
    )

    bucket_name = "triplens-bucket"
    object_name = "raw/rest_countries.json"

    try:
        # Create a bucket if it doesn't already exist
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' created successfully")
        else:
            print(f"Bucket '{bucket_name}' already exists.")

        # Upload a local file to the bucket
        client.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=data_stream,
            content_type="application/json",
            length=len(data)
        )
        print("File uploaded successfully")

    except S3Error as e:
        print(f"An error occurred: {e}")

    return None

upload()