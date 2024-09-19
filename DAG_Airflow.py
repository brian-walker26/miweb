from airflow import DAG
from airflow import models
from airflow.providers.google.cloud.operator.functions import CloudFunctionInvokeFunctionInvokeOperator
from datetime import datetime

REGION = 'northamerica-northeast2'
PROJECT_ID = 'eloquent-grail-429722-c2'

DEFAULT_ARGS = {
    'owner' : 'Brian',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'email_on_failure': False,
    'email_on_retry' : False
    }

with DAG(
    'Webinar_serverless',
    default_args=DEFAULT_ARGS,
    description='Data Pipeline with Paralelismo & Concurrencia',
    catchup=False,
    start_date=datetime(2024, 9, 9),
    schedule_interval='@once',
    tags=['GCP', 'DATAPROC', 'BIGQUERY']


) as dag:
    ingest_trx = CloudFunctionInvokeFunctionOperator(
        task_id= 'ingest_trx',
        function_id = 'func_transacciones',
        input_data = None,
        Location = REGION,
        project_id = PROJECT_ID
    )

    ingest_cmp = CloudFunctionInvokeFunctionOperator(
        task_id= 'ingest_cmp',
        function_id = 'func_campaniass',
        input_data = None,
        Location = REGION,
        project_id = PROJECT_ID
    )

        ingest_trx
        ingest_cmp 