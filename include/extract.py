import requests
from dotenv import load_dotenv
import os
import json
from pathlib import Path
from minio import Minio
from minio.error import S3Error

load_dotenv()

API_KEY=os.getenv("API_KEY")
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
            with open("sample_country.json", "w") as fn:
                json.dump(response.json(), fn, indent=2) # Add API data to a new JSON file
                print("Successfully added JSON data into new json file")

        else:
            print(f"API Error Response: {response.text}")

    except Exception as e:
        print(e)

    return None

def upload():

    # request_api()

    location = Path(__file__).resolve().parent # Location (folder name) where this file belongs

    project_root = location.parent # Name of the project's folder

    LOCAL_FILE_PATH = project_root / "sample_country.json"

    # Initialize the client
    client = Minio(
        endpoint="host.docker.internal:9000",       
        access_key="adminadmin",    
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
        client.fput_object(
            bucket_name=bucket_name,
            object_name=object_name,
            file_path=LOCAL_FILE_PATH
        )
        print("File uploaded successfully")

        # Download the file back down to verify
        client.fget_object(
            bucket_name=bucket_name,
            object_name=object_name,
            file_path=LOCAL_FILE_PATH
        )
        print("File downloaded successfully!")

    except S3Error as e:
        print(f"An error occurred: {e}")

    return None