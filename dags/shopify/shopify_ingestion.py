import json
import logging
from typing import Literal, TypedDict
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.models import DagRun, Variable
from airflow.decorators import dag, task
from datetime import datetime, timedelta, timezone, time
from airflow.timetables.base import DagRunInfo, DataInterval
from airflow.operators.latest_only import LatestOnlyOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.exceptions import AirflowException
from airflow.utils.trigger_rule import TriggerRule
from utils.operators.lambda_function import LambdaInvokeFunctionOperator
from timetables.latest_thirty_otherwise_daily_timetable import LatestHalfHourOtherwiseDailyTimetable
from timetables.continuous_and_catchup_timetable import ContinuousAndCatchupTimetable
from operators.aws_glue_run_crawler_operator import AwsGlueCrawlerOperator
from airflow.timetables.base import Timetable

from utils.lambda_client import LambdaClient, DEFAULT_LAMBDA_CONFIG

logger: logging.Logger = logging.getLogger(__name__)

# =====================================================================
# ========================= Configuration =============================
# =====================================================================

every_30_minutes = '*/30 * * * *'
every_6_hours = '0 */6 * * *'
worker_types = Literal['G.1X', 'G.2X', 'G.4X', 'G.8X']
class IngestionConfig(TypedDict):
    page_size: int
    schedule: str | Timetable
    backfill_schedule: str | Timetable
    num_concurrent_dags: int
    additional_tags: list[str]
    api_versions: list[str]
    worker_type: worker_types
    num_workers: int
    full_rebuild_worker_type: worker_types
    full_rebuild_num_workers: int
    glue_job_base: str
    
worker_type_size_order = ['G.1X', 'G.2X', 'G.4X', 'G.8X']

object_configs: dict[str,IngestionConfig] = {
    'order': IngestionConfig(
        page_size = 250,
        schedule = ContinuousAndCatchupTimetable(
            continuous_interval=timedelta(minutes=30),
            catchup_interval=timedelta(days=1),
        ),
        backfill_schedule = '@daily',
        num_concurrent_dags = 2,
        additional_tags = [],
        api_versions=['2024-04', '2024-10'],
        worker_type='G.2X',
        num_workers=10,
        full_rebuild_worker_type='G.4X',
        full_rebuild_num_workers=10,
        glue_job_base='flatten_order'
        ),
    'order_refund': IngestionConfig(
        page_size = 250,
        schedule = ContinuousAndCatchupTimetable(
            continuous_interval=timedelta(minutes=30),
            catchup_interval=timedelta(days=1),
        ),
        backfill_schedule = '@daily',
        num_concurrent_dags = 10,
        additional_tags = [],
        api_versions=['2024-10'],
        worker_type='G.1X',
        num_workers=5,
        full_rebuild_worker_type='G.2X',
        full_rebuild_num_workers=10,
        glue_job_base='flatten_order_refund'
        ),
    'order_connections': IngestionConfig(
        page_size = 10,
        schedule = ContinuousAndCatchupTimetable(
            continuous_interval=timedelta(minutes=30),
            catchup_interval=timedelta(hours=1),
        ),
        backfill_schedule = every_6_hours,
        num_concurrent_dags = 1,
        additional_tags = [],
        api_versions=['2024-04', '2024-10'],
        worker_type='G.2X',
        num_workers=10,
        full_rebuild_worker_type='G.4X',
        full_rebuild_num_workers=10,
        glue_job_base='flatten_order_connection'
        ),
    'product': IngestionConfig(
        page_size = 250,
        schedule = ContinuousAndCatchupTimetable(
            continuous_interval=timedelta(minutes=30),
            catchup_interval=timedelta(days=1),
        ),
        backfill_schedule = '@monthly',
        num_concurrent_dags = 10,
        additional_tags = [],
        api_versions=['2024-10'],
        worker_type='G.2X',
        num_workers=10,
        full_rebuild_worker_type='G.4X',
        full_rebuild_num_workers=10,
        glue_job_base='flatten_product'
        ),
    'product_variant': IngestionConfig(
        page_size = 250,
        schedule = ContinuousAndCatchupTimetable(
            continuous_interval=timedelta(minutes=30),
            catchup_interval=timedelta(days=1),
        ),
        backfill_schedule = '@monthly',
        num_concurrent_dags = 10,
        additional_tags = [],
        api_versions=['2024-10'],
        worker_type='G.2X',
        num_workers=10,
        full_rebuild_worker_type='G.4X',
        full_rebuild_num_workers=10,
        glue_job_base='flatten_product_variant'
        ),
}

for object_name, config in object_configs.items():
    # Environment
    ENV = Variable.get(key='env', default_var='dev')
    # Object name to extract
    OBJECT_NAME = object_name
    # Transform job name
    TRANSFORM_JOB_NAME = config['glue_job_base'] if ENV == "prd" else f"{config['glue_job_base']}_dev"
    # Extractor
    LAMBDA_FUNCTION = f"shopify-graphql-admin-api-{OBJECT_NAME.replace('_','-')}-{ENV}"
    # When should we backfill from?
    START_DATE = datetime(2024,11,1,tzinfo=timezone.utc)
    BACKFILL_START_DATE = datetime(2016,1,1,tzinfo=timezone.utc)
    BACKFILL_END_DATE = START_DATE
    # Extraction
    PAGE_SIZE = config['page_size']
    MAX_CONCURRENT_CALLS = 10
    # Payload params
    if ENV == "dev":
        SHOP_API_TOKEN_SECRET_NAME = f"prod/shopify/data-engineering-shopify-extract-dev/api-tokens"
        SHOP_API_BACKFILL_TOKEN_SECRET_NAME = "prod/shopify/data-engineering-shopify-extract-dev/api-tokens"
    else:
        SHOP_API_TOKEN_SECRET_NAME = f"prod/shopify/data-engineering-shopify-extract/api-tokens"
        SHOP_API_BACKFILL_TOKEN_SECRET_NAME = "prod/shopify/data-engineering-shopify-extract-backfill/api-tokens"
    DEFAULT_API_VERSION = '2024-04'
    # Schedule
    SCHEDULE = config['schedule']
    BACKFILL_SCHEDULE = config['backfill_schedule']
    # Vacuum Time Period
    VACUUM_TIME = time(hour=23,tzinfo=ZoneInfo(key="America/Los_Angeles"))
    # Timeout
    DAG_TIMEOUT = timedelta(hours=3)
    # Tags
    TAGS = ['graphql', 'shopify'] + config['additional_tags']
    # MAX ACTIVE DAG RUNS
    MAX_DAG_RUNS = config['num_concurrent_dags']
    # Default Task Arguments
    DEFAULT_ARGS = {
        "owner": "airflow",
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 3,
        "max_retry_delay": 60,
    }
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
    DOC_MD = f"""
    ## Shopify {OBJECT_NAME} Batch ELT

    ELT pipeline ingesting data using Shopify's GraphQL Admin API
    """
        

    # =====================================================================
    # ========================== Define DAG ===============================
    # =====================================================================

    def ingest_shopify_data(
        api_version: str, 
        shop_api_token_secret_name: str,
        page_size: int = 250,
    ):  
        @task
        def prepare_payload(
                shop_name: str,
                shop_api_token_secret_name: str = SHOP_API_TOKEN_SECRET_NAME,
                api_version: str = DEFAULT_API_VERSION,
                page_size: int = PAGE_SIZE,
                data_interval_start: datetime|None = None, 
                data_interval_end: datetime|None = None, 
                **kwargs
            ) -> str | list[str]:
                # Ensure a data range is set
                if data_interval_start is None or data_interval_end is None:
                    raise ValueError('data_interval_start, data_interval_end, or both were None.')
                    
                complete_overwrite = data_interval_end - data_interval_start == timedelta(days=1)
                    
                payload = {
                    "shop_name": shop_name,
                    "shop_api_token_secret_name": shop_api_token_secret_name,
                    "api_version": api_version,
                    "page_size": page_size,
                    "max_concurrent_calls": MAX_CONCURRENT_CALLS,
                    "start_timestamp": data_interval_start.isoformat(),
                    "end_timestamp": data_interval_end.isoformat(),
                    "complete_overwrite": complete_overwrite,
                    "use_async": True,
                }
                serialized_payload = json.dumps(payload,default=str)
                msg = f'''
                
    =======================================
    Serialized Payload:
    {json.dumps(payload,default=str,indent=4)}
    =======================================
                '''
                logger.info(msg)
                return serialized_payload
            
        @task(execution_timeout=timedelta(minutes=16))
        def invoke_lambda(
            function_name,
            payload,
        ) -> str:
            
            payload = json.loads(payload)
            msg = f'''
            
            =======================================
            Invoking: {function_name}
            Payload:
{payload}
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
            
            print(f"""
            =======================================
            lambda response:
            
            {response}
            =======================================
            """)
            
            return json.dumps(response)
            

        @task
        def get_file_uris(lambda_response):
            response_body = json.loads(json.loads(lambda_response)['body'])
            print(response_body)
            status_msg = f'''
            
            =======================================
            file_uri: {response_body.get('file_uri')}
            file_uris: {response_body.get('file_uris')}
            number_of_records: {response_body['number_of_records']}
            start_timestamp: {response_body['start_timestamp']}
            end_timestamp: {response_body['end_timestamp']}
            min_updated_at: {response_body['min_updated_at']}
            max_updated_at: {response_body['max_updated_at']}
            time_taken_seconds: {response_body['time_taken_seconds']}
            =======================================
            '''
            logger.info(status_msg)
            file_uris = response_body.get('file_uris')
            if isinstance(file_uris, str):
                return [file_uris]
            elif isinstance(file_uris, list):
                return file_uris
            else:
                raise AirflowException(f'Unexpected type for file_uris: {type(file_uris)}')
            return 
        
        @task
        def gather_file_uris(*uri_lists: list[str], **kwargs) -> list[str]:
            files: list[str] = []
            for uri_list in uri_lists:
                logger.info(f'{uri_list = }')
                for file in uri_list:
                    files.append(file.replace('s3://','s3a://'))
            msg = f'''
                
    =======================================
    {files = }
    =======================================
                '''
            logger.info(msg)
            return files
        
        @task
        def in_vacuum_time_window(
            # set by airflow
            # ==============
            data_interval_start: datetime|None = None, 
            data_interval_end: datetime|None = None, 
            **kwargs
            # ==============
            ) -> bool:
            """
            Checks if the current execution time is within the vacuum/optimization window.
            """
            def is_time_between(t: time, start: datetime, end: datetime) -> bool:
                start_time = start.time()
                end_time = end.time()
                if start_time <= end_time:
                    return start_time <= t <= end_time
                else:  # crosses midnight
                    return t >= start_time or t <= end_time
            
            in_time_window = is_time_between(
                t=VACUUM_TIME, 
                start=data_interval_start, 
                end=data_interval_end
            )
            
            logger.info(f"In vacuum time window: {in_time_window}")
            return in_time_window
            
        @task.branch()
        def which_transformation(
            files: list[str], 
            in_time_window: bool,
            
            # set by airflow
            # ==============
            dag_run: DagRun|None = None,
            dag: DAG|None = None,
            data_interval_start: datetime|None = None,
            data_interval_end: datetime|None = None,
            ts: datetime|None = None,
            **kwargs
            # ==============
            ) -> bool:
            """
            Decides whether to proceed with the transformation.
            
            Should run optimize if:
            - it's a backfill run and it's the last time period in the backfill
            - it's a catchup run and it's the last run of the catchup window
            - it's a normal run and the time period falls within the optimization/vacuum window
            
            Should run without optimize if:
            - it's a normal run and files exist
            
            Skip transformation as default
            """
            run_as_backfill = "_backfill" in dag.dag_id

            if run_as_backfill:
                if data_interval_end == BACKFILL_END_DATE:
                    # it's a backfill run and it's the last time period in the backfill
                    return 'prepare_glue_script_args_optimize'
            else:
                time_window_delta: timedelta = data_interval_end - data_interval_start
                
                # it's a catchup run if "now" is not in the next time window
                is_catchup_run: bool = (data_interval_end + time_window_delta) < datetime.now(timezone.utc)
                
                if is_catchup_run:
                    next_next_data_interval_start = data_interval_end + time_window_delta
                    next_next_data_interval_end = next_next_data_interval_start + time_window_delta
                    if next_next_data_interval_start < datetime.now(timezone.utc) < next_next_data_interval_end:
                        # it's a catchup run and it's the last run of the catchup window
                        return 'prepare_glue_script_args_optimize'
                    else:
                        return 'end'
                else:
                    if in_time_window:
                        # it's a normal run and the time period falls within the optimization/vacuum window
                        return 'prepare_glue_script_args_optimize'
                    elif files:
                        # it's a normal run and files exist
                        return 'prepare_glue_script_args'
                    else:
                        return 'end'
        
        @task
        def prepare_glue_script_args_optimize() -> dict:
            script_args = {
                '--env': ENV,
                '--api_version': api_version,
                '--vacuum': str(True),
                '--optimize': 'full',
                '--additional-python-modules': 'backoff==2.2.1',
                '--extra-py-files': 's3://prd-mde-datalake/glue_jobs/ruggable_de.zip',
            }
            logger.info(f"""
                        
                =======================================
                {script_args = }
                =======================================
                """)
            return script_args
        
        @task
        def prepare_glue_script_args(**kwargs) -> dict:
            files = kwargs["ti"].xcom_pull(task_ids="gather_file_uris")
            script_args = {
                '--env': ENV,
                '--api_version': api_version,
                '--additional-python-modules': 'backoff==2.2.1',
                '--extra-py-files': 's3://prd-mde-datalake/glue_jobs/ruggable_de.zip',
                '--file_names': json.dumps(files)
            }
            logger.info(f"""
                        
                =======================================
                {script_args = }
                =======================================
                """)
            return script_args
        
        @task
        def trigger_glue_crawler(**kwargs):
            crawler_name = f'{ENV}_shopify_{api_version}_crawler'
            run_glue = AwsGlueCrawlerOperator(
                task_id='trigger_glue_crawler',
                crawler_name=crawler_name
            )
            run_glue.execute(context={})
            return {'glue_crawler_name': crawler_name}
        
        start_task = EmptyOperator(task_id='start')
        
        # Invoke the lambda for each store and gather ingested files
        file_uri_lists = []
        for shop_name in SHOP_NAMES:
            payload = prepare_payload.override(
                # Override the task name to be shop specific
                task_id=f"{shop_name}_payload"
                )(
                shop_name = shop_name,
                shop_api_token_secret_name = shop_api_token_secret_name,
                api_version = api_version,
                page_size = page_size,
            )
            lambda_response = invoke_lambda.override(task_id=f"{shop_name}_{OBJECT_NAME}_ingestion")(
                function_name = LAMBDA_FUNCTION,
                payload = payload)
            ingested_files = get_file_uris.override(task_id=f"{shop_name}_files")(lambda_response)
            file_uri_lists.append(ingested_files)
            start_task >> payload
        
        files = gather_file_uris(*file_uri_lists)
        is_in_vacuum_time_window = in_vacuum_time_window()
        start_task >> is_in_vacuum_time_window
        
        transformation_choice = which_transformation(files=files, in_time_window=is_in_vacuum_time_window)
            
        script_args = prepare_glue_script_args()
        optimize_script_args = prepare_glue_script_args_optimize()
        
        end_task = EmptyOperator(task_id='end',trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
        transformation_choice >> end_task
        
        for suffix, arguments in {
            '': {
                'script_args': script_args,
                'run_job_kwargs': {
                    "WorkerType": config['worker_type'],
                    "NumberOfWorkers": config['num_workers'],
                }}, 
            '_with_optimize': {
                'script_args': optimize_script_args,
                'run_job_kwargs': {
                    "WorkerType": config['full_rebuild_worker_type'],
                    "NumberOfWorkers": config['full_rebuild_num_workers'],
                }}
        }.items():
            transformation_choice >> arguments['script_args']
            
            transform = GlueJobOperator(
                task_id=f'{TRANSFORM_JOB_NAME}{suffix}',
                job_name=TRANSFORM_JOB_NAME,
                region_name="us-west-2",
                iam_role_name="AWSGlueServiceRole-S3-Access",
                concurrent_run_limit=10,
                retry_limit=0,
                script_args=arguments['script_args'],
                run_job_kwargs=arguments['run_job_kwargs'],
                execution_timeout=timedelta(minutes=60)
            )
            
            if suffix == '_with_optimize':
                glue_crawler = trigger_glue_crawler()
                transform >> glue_crawler >> end_task
            else:
                transform >> end_task

        
    for api_version in config['api_versions']:
        @dag(
            dag_id=f'shopify_{OBJECT_NAME}_{api_version}',
            description=f'Orchestrate Shopify {OBJECT_NAME} ELT',
            schedule=SCHEDULE,
            start_date=START_DATE,
            tags=TAGS + [f'admin_api_{api_version.replace("-", "_")}'],
            catchup=True,
            default_args=DEFAULT_ARGS,
            dagrun_timeout = DAG_TIMEOUT,
            max_active_runs=MAX_DAG_RUNS,
            doc_md = DOC_MD, 
        )
        def forward():
            ingest_shopify_data(
                api_version=api_version,
                shop_api_token_secret_name=SHOP_API_TOKEN_SECRET_NAME,
                page_size=PAGE_SIZE,
                )
            
        @dag(
            dag_id=f'shopify_{OBJECT_NAME}_{api_version}_backfill',
            description=f'Orchestrate Shopify {OBJECT_NAME} Backfill',
            schedule=BACKFILL_SCHEDULE,
            start_date=BACKFILL_START_DATE,
            end_date=BACKFILL_END_DATE,
            tags=TAGS + [f'admin_api_{api_version.replace("-", "_")}'],
            catchup=True,
            default_args=DEFAULT_ARGS,
            dagrun_timeout = DAG_TIMEOUT,
            max_active_runs=MAX_DAG_RUNS,
            doc_md = DOC_MD,
        )
        def backward():
            ingest_shopify_data(
                api_version=api_version,
                shop_api_token_secret_name=SHOP_API_BACKFILL_TOKEN_SECRET_NAME,
                page_size=PAGE_SIZE,
            )
        
        forward()
        backward()