from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr_job_flow import EmrJobFlowSensor

import logging

from utils import load_yml_config

config = load_yml_config( os.path.join("config", 'etl.yml') )

s3 = config.get("aws").get('s3')
redshift = config.get("aws").get('redshift')
airflow = config.get('airflow')

REGION = config.get("aws").get("region")
REDSHIFT_SCHEMA = redshift.get('schema')
REDSHIFT_DATABASE = redshift.get('database')
S3_BUCKET = s3.get('bucket')
S3_LOG_KEY = s3.get('log_data_key')
S3_SONG_KEY = s3.get('song_data_key')
AIRFLOW_REDSHIFT_CONNECTION = airflow.get('redshift_conn_id')
AIRFLOW_AWS_CREDENTIALS = airflow.get('aws_credentials_id')
AIFLOW_DAG_ID = 'etl_pipeline_immigration'

logging.info(f"Instanciating DAG {AIFLOW_DAG_ID}")


default_args = {
    'owner': 'Matheus',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2018, 11, 1),
    'email_on_retry': False,
    'catchup': False,
}

with DAG(AIFLOW_DAG_ID,
        default_args=default_args,
        description="Load and transform data in ",
        schedule_interval=None,
        tags=['udacity', 'immigration']
        ) as dag:

    start_operator = DummyOperator(task_id='begin_execution')



