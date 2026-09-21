import os
from dotenv import load_dotenv
import boto3
from snowflake.connector import connect
from pathlib import Path

load_dotenv()

# Snowflake config
SF_USER=os.getenv("SF_USER")
SF_PASS=os.getenv("SF_PASS")
SF_ACCOUNT=os.getenv("SF_ACCOUNT")

# MinIO config
MINIO_ACCOUNT=os.getenv("MINIO_ACCOUNT")
MINIO_KEY=os.getenv("MINIO_KEY")
minio_endpoint = "http://localhost:9000"
minio_bucket = "triplens-bucket"
minio_object = "raw/rest_countries.json"

# Establish initial connection to Snowflake
conn = connect(
    user=SF_USER,
    password=SF_PASS,
    account=SF_ACCOUNT,
    warehouse="COMPUTE_WH",
    database="TRIPLENS",
    schema="TRIPLENS.RAW"
)

cursor = conn.cursor()

# Setup boto3 client
s3_client = boto3.client(
    "s3",
    endpoint_url=minio_endpoint,
    aws_access_key_id=MINIO_ACCOUNT,
    aws_secret_access_key=MINIO_KEY,
    config=boto3.session.Config(signature_version="s3v4"), # Sets signature version of boto3 to MinIO compatible one
    verify=False
)

def load_minio_to_snowflake() -> None:
    """Loads JSON data saved in the MinIO bucket directly to Snowflake"""

    location = Path(__file__).resolve().parent # Location (folder name) where this file belongs

    file_name = os.path.basename(minio_object) # Name of the project's folder

    local_temp_file = location / file_name

    s3_client.download_file(minio_bucket, minio_object, local_temp_file)
    print("Downloaded temporary json file from MinIO")

    try:
        # Create the Database and Schemas
        cursor.execute("CREATE DATABASE IF NOT EXISTS RAW;")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS RAW.STAGE_ZONE;")
        cursor.execute("USE SCHEMA RAW.STAGE_ZONE")
        
        # Create a JSON File Format to read the raw MinIO JSON files
        cursor.execute("""
            CREATE OR REPLACE FILE FORMAT RAW.STAGE_ZONE.JSON_FORMAT
            TYPE = 'JSON'
            STRIP_OUTER_ARRAY = TRUE
            IGNORE_UTF8_ERRORS = TRUE;
        """)

        # Create the Landing Table with a VARIANT column
        cursor.execute("""
            CREATE OR REPLACE TABLE RAW.STAGE_ZONE.STG_COUNTRIES_RAW (
                raw_data VARIANT,
                loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                file_name STRING
            );
        """)

        # Create an Internal Stage
        cursor.execute("""
            CREATE OR REPLACE STAGE RAW.STAGE_ZONE.INTERNAL_JSON_STAGE
            FILE_FORMAT = RAW.STAGE_ZONE.JSON_FORMAT;
        """)

        print("Infrastructure setup complete")


        # Stream object content into Snowflake internal stage
        print(f"Staging {minio_object} from MinIO into Snowflake internal stage")
        cursor.execute(f"PUT file://{local_temp_file} @INTERNAL_JSON_STAGE AUTO_COMPRESS=TRUE OVERWRITE=TRUE")

        # Copy data from MinIO stage directly into the VARIANT column
        print("Copying data into STG_COUNTRIES_RAW")
        cursor.execute("TRUNCATE TABLE STG_COUNTRIES_RAW")
        cursor.execute(f"""
            COPY INTO RAW.STAGE_ZONE.STG_COUNTRIES_RAW (raw_data, loaded_at, file_name)
            FROM (
                SELECT 
                    src.$1, 
                    CURRENT_TIMESTAMP(),
                    METADATA$FILENAME 
                FROM @RAW.STAGE_ZONE.INTERNAL_JSON_STAGE src
            )
            FILE_FORMAT = (FORMAT_NAME = 'RAW.STAGE_ZONE.JSON_FORMAT')
            ON_ERROR = 'ABORT_STATEMENT';
        """)
        
        # Print out summary of the load
        results = cursor.fetchall()
        for row in results:
            print(f"Load Result: {row}")

    finally:
        cursor.close()
        conn.close()
        print("Snowflake connection closed")
        if os.path.exists(local_temp_file):
            os.remove(local_temp_file)
        print("Removed local temporary JSON file")