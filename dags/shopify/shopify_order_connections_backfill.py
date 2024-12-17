import json
import logging

from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta, timezone
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowException
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

from utils.lambda_client import LambdaClient, DEFAULT_LAMBDA_CONFIG

doc_md = """
## Shopify Orders Connections Batch Backfill EL

EL pipeline ingesting data using Shopify's GraphQL Admin API
"""

logger: logging.Logger = logging.getLogger(__name__)

# =====================================================================
# ========================= Configuration =============================
# =====================================================================

# Environment
ENV = "dev" # TODO: Change when going to prod
# API VERSION
API_VERSION = '2024-04'
# REDSHIFT connection
REDSHIFT_CONN_ID = 'redshift_connection'
# Extraction Side
MAX_PAGES = 500
PAGE_SIZE = 10
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
):
    msg = f'''
    
    =======================================
    Invoking: {function_name}
    Max Returned Records: {payload['max_pages'] * payload['page_size']}
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
    status: {response_body['status']}
    cursor: {response_body['cursor']}
    =======================================
    '''
    logger.info(status_msg)
    return response_body
    

# =====================================================================
# ========================== Define DAG ===============================
# =====================================================================

with DAG(
    dag_id='shopify_graphql_order_connections_backfill',
    description='Orchestrate Shopify Order Connections data backfill',
    schedule='* * * * *',
    start_date=datetime(2024,1,1,tzinfo=timezone.utc),
    tags=['graphql', 'backfill', 'shopify'],
    default_args = default_args,
    dagrun_timeout = timedelta(minutes=20),
    max_active_runs=1,
    doc_md = doc_md,
) as dag:
    
    # Start tasks
    start_task = EmptyOperator(task_id='start')
    
    # Invoke the lambda for each store
    lambda_tasks = []
    for shop_name, payload in payloads.items():
        response = invoke_lambda.override(
            # Override the task name to be shop specific
            task_id=f'{shop_name}_extraction'
            )(
            function_name=f'shopify-graphql-admin-api-order-connections-backfill-el-{ENV}',
            payload=payload,
        )
        logger.info('lambda response', extra={'response': response})
        lambda_tasks.append(response)
        
    # Empty end task to aggregate all the successful task executions
    end_task = EmptyOperator(task_id='end', trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    
    # Orchestrate    
    start_task >> lambda_tasks >> end_task
    