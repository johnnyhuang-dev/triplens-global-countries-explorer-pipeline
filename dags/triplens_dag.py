from datetime import datetime, timedelta

from airflow import DAG
from airflow.sdk import task

from include.extract import upload

default_args = {
    'owner': 'orproja',
    'depends_on_past': False, # prevent airflow from executing previously missed tasks
    'start_date': datetime(2026, 9, 11),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'schedule_interval': '@weekly',
}

@task
def extract_api_data():
    api_response = upload()

    return api_response

with DAG(dag_id='triplens_dag',
         catchup=False,
         default_args=default_args):

    extract_api_data()