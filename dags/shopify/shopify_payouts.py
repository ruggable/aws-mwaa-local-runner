import json
import logging
from typing import TypedDict

from airflow import DAG
from airflow.decorators import task, task_group, dag
from datetime import datetime, timedelta, timezone
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.exceptions import AirflowException
from airflow.utils.trigger_rule import TriggerRule

from utils.lambda_client import LambdaClient, DEFAULT_LAMBDA_CONFIG

doc_md = """
## Shopify Payouts Batch ELT

ELT pipeline ingesting data using Shopify's GraphQL Admin API
"""

logger: logging.Logger = logging.getLogger(__name__)

# =====================================================================
# ========================= Configuration =============================
# =====================================================================

# Environment
ENV = "dev"
# API VERSION
API_VERSIONS = ['2024-10']
# REDSHIFT connection
REDSHIFT_CONN_ID = 'redshift_connection'
# Scheduling details
RUN_FREQUENCY_MINUTES = 60 * 24 # 1 day
SCHEDULE = timedelta(minutes=RUN_FREQUENCY_MINUTES)
START_DATE = datetime(2024,1,1,tzinfo=timezone.utc)
TAGS=['graphql', 'backfill', 'shopify']
# Shopify API Token Secret Manager Name
SECRET_NAME = 'prod/shopify/data-engineering-shopify-extract/api-tokens'
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

class Payload(TypedDict):
    shop_api_token_secret_name: str
    api_version: str
    shop_name: str
    

payload_per_api_version_per_shop: dict[str,dict[str,Payload]] = {}
for api_version in API_VERSIONS:
    payload_per_shop: dict[str,Payload] = {}
    for shop_name in set([shop_name.lower() for shop_name in SHOP_NAMES]):
        payload_per_shop[shop_name] = Payload(
            shop_api_token_secret_name=SECRET_NAME,
            api_version=api_version,
            shop_name=shop_name,
        )
    payload_per_api_version_per_shop[api_version] = payload_per_shop
    
    
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

@dag(
    dag_id='shopify_graphql_payouts_el',
    description='Orchestrate Shopify Payout data EL',
    schedule=SCHEDULE,
    start_date=START_DATE,
    tags=TAGS,
    catchup=False,
    default_args = default_args,
    dagrun_timeout = timedelta(minutes=20),
    max_active_runs=1,
    doc_md = doc_md,
)
def extract_shopify_payouts():
    
    # Start tasks
    start_task = EmptyOperator(task_id='start')
    
    # Empty end task to aggregate all the successful task executions
    end_task = EmptyOperator(task_id='end', trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    
    # Create a task group for each API version
    for api_version, payloads_per_shop in payload_per_api_version_per_shop.items():
        @task_group(group_id=f'extract_payouts_api_version_{api_version}')
        def extract_payouts():
            # Extract: Invoke the lambda for each store
            file_uris = []
            for shop_name, payload in payloads_per_shop.items():
                response = invoke_lambda.override(
                    # Override the task name to be shop specific
                    task_id=f'{api_version}_{shop_name}_extraction'
                    )(
                    function_name=f'shopify-graphql-admin-api-payout-el-{ENV}',
                    payload=payload,
                )
                file_uris.append(response)
                
            gathered_file_uris = gather_file_uris(*file_uris)
        
            # Transform
            transform = GlueJobOperator(
                task_id='transform',
                job_name="flatten_payouts",
                region_name="us-west-2",
                iam_role_name="AWSGlueServiceRole-S3-Access",
                concurrent_run_limit=1,
                retry_limit=3,
                script_args={
                    '--env': ENV,
                    '--file_names': gathered_file_uris,
                    '--api_version': api_version,
                },
            )
            
            # Orchestrate    
            file_uris >> gathered_file_uris >> transform
        start_task >> extract_payouts() >> end_task

# Run DAG
extract_shopify_payouts()