import json
import logging

from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta, timezone
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.exceptions import AirflowException
from airflow.utils.trigger_rule import TriggerRule

from utils.lambda_client import LambdaClient, DEFAULT_LAMBDA_CONFIG

doc_md = """
## Shopify Order Connections Batch ELT

ELT pipeline ingesting data using Shopify's GraphQL Admin API
"""

logger: logging.Logger = logging.getLogger(__name__)

# =====================================================================
# ========================= Configuration =============================
# =====================================================================

# Environment
ENV = "dev"
# API VERSION
API_VERSION = '2024-04'
# REDSHIFT connection
REDSHIFT_CONN_ID = 'redshift_connection'
# Extraction Side
MAX_PAGES = 500
PAGE_SIZE = 10
RUN_FREQUENCY_MINUTES = 15
# Shops to process
SHOP_NAMES: list[str] = [
    'ruggable',
    'eu-ruggable',
    'uk-ruggable',
    'au-ruggable',
    'germany-ruggable',
    'kr-ruggable',
]

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.now(timezone.utc)-timedelta(days=2),
    "catchup": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=30),
    "max_retry_delay": 60,
    "sla": timedelta(minutes=20),
    "wait_for_downstream": True,
    "trigger_rule": TriggerRule.ALL_SUCCESS
}

base_payload: dict[str,str] = {
    "shop_api_token_secret_name": "prod/shopify/data-engineering-shopify-extract/api-tokens",
    "api_version": "2024-04",
    "max_pages": MAX_PAGES,
    "page_size": PAGE_SIZE
}

payloads: dict[str,dict[str,str]] = {}
deduped_shop_names = set([shop_name.lower() for shop_name in SHOP_NAMES])
for shop_name in deduped_shop_names:
    shop_payload = base_payload.copy()
    shop_payload['shop_name'] = shop_name
    payloads[shop_name] = shop_payload
    
    
# =====================================================================
# ========================= Custom Tasks ==============================
# =====================================================================
            
@task
def invoke_lambda(
    function_name: str,
    payload: dict[str, str],
) -> str:
    msg = f'''
    
    =======================================
    Invoking: {function_name}
    Max Returned Records: {payload['max_pages'] * payload['page_size']}
    Max Processing Speed: {payload['max_pages'] * payload['page_size'] / RUN_FREQUENCY_MINUTES} records/minute
    =======================================
    '''
    logger.info(msg)
    
    # invoke lambda
    serialized_payload = json.dumps(payload)
    response = LambdaClient.invoke(function_name=function_name, payload=serialized_payload)
    
    # check for errors
    if not isinstance(response, dict):
        msg = f'Unexpected response type for lambda function response. Expected a dict but got: {type(function_name)}'
        logger.exception(msg)
        if isinstance(response, str):
            logger.exception(f'Response: {response}')
        raise AirflowException(msg)
    if response.get('status_code') != 200 or 'body' not in response:
        msg = f'Internal lambda error: {json.dumps(response, default=str)}'
        logger.exception(msg)
        raise AirflowException(msg)
    
    # parse response
    msg = f'Response body: {response["body"]}'
    logger.info(msg)
    response_body = json.loads(response['body'])
    status_msg = f'''
    
    =======================================
    file_uri: {response_body['file_uri']}
    number_of_records: {response_body['number_of_records']}
    start_timestamp: {response_body['start_timestamp']}
    end_timestamp: {response_body['end_timestamp']}
    min_updated_at: {response_body['min_updated_at']}
    max_updated_at: {response_body['max_updated_at']}
    time_taken_seconds: {response_body['time_taken_seconds']}
    =======================================
    '''
    logger.info(status_msg)
    return response_body['file_uri']
    
# Combine the file URIs into a list
@task
def gather_file_uris(*uris):
    return json.dumps(list(uris))

# =====================================================================
# ========================== Define DAG ===============================
# =====================================================================

with DAG(
    dag_id='shopify_graphql_order_connections_el',
    description='Orchestrate Shopify Order Connections data EL',
    schedule=timedelta(minutes=RUN_FREQUENCY_MINUTES),
    start_date=datetime(2024,1,1,tzinfo=timezone.utc),
    tags=['graphql', 'backfill', 'shopify'],
    catchup=False,
    default_args = default_args,
    dagrun_timeout = timedelta(minutes=20),
    max_active_runs=1,
    doc_md = doc_md,
) as dag:
    
    # Start tasks
    start_task = EmptyOperator(task_id='start')
    
    # Invoke the lambda for each store
    file_uris = []
    for shop_name, payload in payloads.items():
        response = invoke_lambda.override(
            # Override the task name to be shop specific
            task_id=f'{shop_name}_extraction'
            )(
            function_name=f'shopify-graphql-admin-api-order-connections-el-{ENV}',
            payload=payload,
        )
        file_uris.append(response)
        
    gathered_file_uris = gather_file_uris(*file_uris)

    # Transform
    transform = GlueJobOperator(
        task_id=f'transform',
        job_name=f"flatten_order_connection",
        region_name="us-west-2",
        iam_role_name="AWSGlueServiceRole-S3-Access",
        concurrent_run_limit=1,
        retry_limit=3,
        script_args={
            '--env': ENV,
            '--file_names': gathered_file_uris
        },
    )
    # Empty end task to aggregate all the successful task executions
    end_task = EmptyOperator(task_id='end', trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    
    # Orchestrate    
    start_task >> file_uris >> gathered_file_uris >> transform >> end_task


 
