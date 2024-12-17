import json
import logging

from airflow.decorators import dag, task
from datetime import datetime, timedelta, timezone
from airflow.operators.latest_only import LatestOnlyOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.exceptions import AirflowException
from airflow.utils.trigger_rule import TriggerRule

from utils.lambda_client import LambdaClient, DEFAULT_LAMBDA_CONFIG

doc_md = """
## Shopify Order Custom Attributes Batch ELT

ELT pipeline ingesting data using Shopify's GraphQL Admin API
"""

logger: logging.Logger = logging.getLogger(__name__)

# =====================================================================
# ========================= Configuration =============================
# =====================================================================

# Environment
ENV = "dev"
# When should we backfill from?
START_DATE = datetime(2024,9,1,tzinfo=timezone.utc)
BACKFILL_START_DATE = datetime(2016,1,1,tzinfo=timezone.utc)
BACKFILL_END_DATE = START_DATE
# API VERSION
API_VERSION = '2024-04'
# Extraction
PAGE_SIZE = 250
# Payload params
SHOP_API_TOKEN_SECRET_NAME = "prod/shopify/data-engineering-shopify-extract/api-tokens"
API_VERSION = '2024-04'
# Schedule
SCHEDULE = '*/15 * * * *'
# Tags
TAGS = ['graphql', 'shopify']
# Extractor
LAMBDA_FUNCTION = f'shopify-graphql-admin-api-order-custom-attribute-{ENV}'
# Shops to process
SHOP_NAMES: set[str] = set(
    shop_name.lower() for shop_name in [
        'ruggable',
        'eu-ruggable',
        'uk-ruggable',
        'au-ruggable',
        'germany-ruggable',
        'kr-ruggable',
    ]
)
            
@task
def invoke_lambda(
    function_name: str,
    shop_name: str,
    shop_api_token_secret_name: str = SHOP_API_TOKEN_SECRET_NAME,
    api_version: str = API_VERSION,
    page_size: int = PAGE_SIZE,
    data_interval_start:datetime|None=None, 
    data_interval_end:datetime|None=None, 
    **kwargs
) -> str:
    # Ensure a data range is set
    if data_interval_start is None or data_interval_end is None:
        raise ValueError('data_interval_start, data_interval_end, or both were None.')
    
    msg = f'''
    
    =======================================
    Invoking:         {function_name}
    Shop:             {shop_name}
    API Version:      {api_version}
    Page Size:        {page_size}
    Date Range Start: {data_interval_start.isoformat()}
    Date Range End:   {data_interval_end.isoformat()}
    =======================================
    '''
    logger.info(msg)
    
    # invoke lambda
    serialized_payload = json.dumps({
        "shop_name": shop_name,
        "shop_api_token_secret_name": shop_api_token_secret_name,
        "api_version": api_version,
        "page_size": page_size,
        "start_timestamp": data_interval_start.isoformat(),
        "end_timestamp": data_interval_end.isoformat()
    })
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
def gather_file_uris(*uris, **kwargs):
    msg = f"""
    
    ===========================
    uris: {uris}
    ===========================
    """
    logger.info(msg)
    ti = kwargs["ti"]
    files = json.dumps([uri.replace('s3://','s3a://') for uri in uris if uri])
    msg = f"""
    
    ===========================
    Files to process: {files}
    ===========================
    """
    logger.info(msg)
    ti.xcom_push("order_custom_attribute_files", files)
    return files
    

# =====================================================================
# ========================== Define DAG ===============================
# =====================================================================

def shopify_graphql_order_custom_attributes():
    # Start tasks 
    start_task = EmptyOperator(task_id='start')
    
    # Invoke the lambda for each store
    file_uris = []
    for shop_name in SHOP_NAMES:
        file_uri = invoke_lambda.override(
            # Override the task name to be shop specific
            task_id=f'{shop_name}_extraction'
            )(
            function_name=LAMBDA_FUNCTION,
            shop_name=shop_name
        )
        start_task >> file_uri
        file_uris.append(file_uri)

    # Get a JSON serialized list of s3 file URIs
    files = gather_file_uris(*file_uris)
    
    latest_only = LatestOnlyOperator(task_id='latest_only')
    
    # Transform
    transform = GlueJobOperator(
        task_id=f'transform',
        job_name=f"flatten_order_custom_attribute",
        region_name="us-west-2",
        iam_role_name="AWSGlueServiceRole-S3-Access",
        concurrent_run_limit=10,
        retry_limit=3,
        script_args={
            '--env': ENV,
            '--additional-python-modules': 'backoff==2.2.1',
            '--file_names': "{{ task_instance.xcom_pull(task_ids='gather_file_uris', key='order_custom_attribute_files')}}"
        },
    )
    
    # Empty end task to aggregate all the successful task executions
    end_task = EmptyOperator(task_id='end', trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    
    # Orchestrate    
    files >> latest_only >> transform >> end_task

@dag(
    dag_id='shopify_graphql_order_custom_attributes',
    description='Orchestrate Shopify Order Custom Attributes data ELT',
    schedule=SCHEDULE,
    start_date=START_DATE,
    tags=TAGS,
    catchup=True,
    default_args={
        "owner": "airflow",
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 3,
        "max_retry_delay": 60,
    },
    dagrun_timeout = timedelta(minutes=20),
    max_active_runs=100,
    doc_md = doc_md,
)
def forward():
    shopify_graphql_order_custom_attributes()
    
@dag(
    dag_id='shopify_graphql_order_custom_attributes_backfill',
    description='Orchestrate Shopify Order Custom Attributes data ELT',
    schedule=SCHEDULE,
    start_date=BACKFILL_START_DATE,
    end_date=BACKFILL_END_DATE,
    tags=TAGS,
    catchup=True,
    default_args={
        "owner": "airflow",
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 3,
        "max_retry_delay": 60,
    },
    dagrun_timeout = timedelta(minutes=20),
    max_active_runs=100,
    doc_md = doc_md,
)
def backward():
    shopify_graphql_order_custom_attributes()
    
forward()
backward()