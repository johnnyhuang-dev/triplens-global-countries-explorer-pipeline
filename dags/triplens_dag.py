from datetime import datetime, timedelta

from airflow import DAG
from airflow.sdk import task

from include.extract import upload
from include.load import load_minio_to_snowflake

default_args = {
    'owner': 'orproja',
    'depends_on_past': False, # prevent airflow from executing previously missed tasks
    'start_date': datetime(2026, 9, 11),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'schedule_interval': '@weekly',
}

@task
def extract_data_to_s3():
    upload()

@task
def load_data_to_snowflake():
    load_minio_to_snowflake()

with DAG(dag_id='triplens_dag',
         catchup=False,
         default_args=default_args):

    extract_data_to_s3() >> load_data_to_snowflake()