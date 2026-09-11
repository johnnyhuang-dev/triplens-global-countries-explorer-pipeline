import os
from dotenv import load_dotenv
import boto3
import io
from snowflake.connector import connect
from snowflake.snowpark import Session

load_dotenv()

SF_USER=os.getenv("SF_USER")
SF_PASS=os.getenv("SF_PASS")
SF_ACCOUNT=os.getenv("SF_ACCOUNT")

MINIO_ACCOUNT=os.getenv("MINIO_ACCOUNT")
MINIO_KEY=os.getenv("MINIO_KEY")

# Create an External Stage pointing to your MinIO bucket 
minio_endpoint = "http://localhost:9000"
minio_bucket = "triplens-bucket"
minio_object = "raw/rest_countries.json"
minio_access_key = MINIO_ACCOUNT
minio_secret_key = MINIO_KEY

# Establish initial connection to Snowflake
conn = connect(
    user=SF_USER,
    password=SF_PASS,
    account=SF_ACCOUNT,
    warehouse="COMPUTE_WH",
    role="ACCOUNTADMIN" 
)
cursor = conn.cursor()

try:
    # Create the Database and Schemas
    cursor.execute("CREATE DATABASE IF NOT EXISTS RAW;")
    cursor.execute("CREATE SCHEMA IF NOT EXISTS RAW.STAGE_ZONE;")
    
    # Create a JSON File Format to read the raw MinIO JSON files
    cursor.execute("""
        CREATE FILE FORMAT IF NOT EXISTS RAW.STAGE_ZONE.JSON_FORMAT
        TYPE = 'JSON'
        STRIP_OUTER_ARRAY = TRUE
        IGNORE_UTF8_ERRORS = TRUE;
    """)

    # Create the Landing Table with a VARIANT column
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS RAW.STAGE_ZONE.STG_COUNTRIES_RAW (
            raw_data VARIANT,
            loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            file_name STRING
        );
    """)

    # Create an Internal Stage
    cursor.execute("""
        CREATE STAGE IF NOT EXISTS RAW.STAGE_ZONE.INTERNAL_JSON_STAGE
        FILE_FORMAT = RAW.STAGE_ZONE.JSON_FORMAT;
    """)

    print("Infrastructure setup complete")


    # Setup boto3 client
    s3_client = boto3.client(
        "s3",
        endpoint_url=minio_endpoint,
        aws_access_key_id=MINIO_ACCOUNT,
        aws_secret_access_key=MINIO_KEY,
    )

    # Stream object content directly into memory
    print(f"Streaming {minio_object} from MinIO")
    s3_response = s3_client.get_object(Bucket=minio_bucket, Key=minio_object)
    file_stream = io.BytesIO(s3_response["Body"].read())

    # Establish a Snowpark streaming session
    session = Session.builder.configs({"connection": conn}).create()

    # Upload stream to Snowflake's Internal Stage using Snowpark
    print("Streaming data into Snowflake Internal Stage")
    session.file.put_stream(
        input_stream=file_stream,
        stage_location=f"@RAW.STAGE_ZONE.INTERNAL_JSON_STAGE/{minio_object}",
        overwrite=True
    )

    # Copy data from MinIO stage directly into the VARIANT column
    print("Copying data into STG_COUNTRIES_RAW")

    cursor.execute(f"""
        COPY INTO RAW.STAGE_ZONE.STG_COUNTRIES_RAW (raw_data, file_name)
        FROM (
            SELECT src.$1, METADATA$FILENAME 
            FROM @RAW.STAGE_ZONE.INTERNAL_JSON_STAGE/{minio_object} src
        )
        FILE_FORMAT = (FORMAT_NAME = 'RAW.STAGE_ZONE.JSON_FORMAT')
        ON_ERROR = 'CONTINUE';
    """)
    
    # Print out summary of the load
    results = cursor.fetchall()
    for row in results:
        print(f"File: {row[0]} | Status: {row[1]} | Rows Parsed: {row[2]}")

finally:
    cursor.close()
    conn.close()
    print("Snowflake connection closed")
