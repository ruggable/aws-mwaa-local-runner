import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.providers.ssh.operators.ssh import SSHOperator


doc_md = """

### Flexport Meltano -> ODS etl

Triggers:
- meltano schedule run tap-flexport-{stream} for all streams in /variables/flexport_streams.json
- dbt job 'flexport_gold' to materialize all current data in dbt_flexport schema

tap-flexport https://github.com/ruggable/tap-flexport

"""

tags = ["flexport", "meltano", "ods"] 

# Default DAG parameters
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 10, 31),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1,
}

# Get flexport streams config
flexport_streams = Variable.get(
    "flexport_streams", 
    default_var={
        "flexport_streams": [
            {"stream_name": "bookings"},
            {"stream_name": "booking-line-items"},
            {"stream_name": "commercial-invoices"},
            {"stream_name": "invoices"},
            {"stream_name": "network-locations"},
            {"stream_name": "purchase-orders"},
            {"stream_name": "purchase-order-line-items"},
            {"stream_name": "ports"},
            {"stream_name": "products"},
            {"stream_name": "ocean-shipment-containers"},
            {"stream_name": "ocean-shipment-container-legs"},
            {"stream_name": "shipments"},
            {"stream_name": "shipment-legs"}
        ]
    }, 
    deserialize_json=True)


with DAG(
        default_args=default_args,
        dag_id="flexport_elt",
        schedule_interval='25 9,13 * * *',  # twice daily at 9:25 and 13:25
        dagrun_timeout=timedelta(minutes=120),
        catchup=False,
        tags=tags,
        doc_md=doc_md
) as dag:
    # Dummy start task
    start = EmptyOperator(
        dag=dag,
        task_id="start"

    )

    # Dummy end task
    end = EmptyOperator(
        dag=dag,
        task_id="end"

    )

    end_ingest = EmptyOperator(
        dag=dag,
        task_id="end_ingest"

    )

    run_dbt_cloud_job = DbtCloudRunJobOperator(
        task_id='run_dbt_cloud_job',
        dbt_cloud_conn_id='dbt_cloud_conn',
        account_id=35124,
        job_id=211890,
        dag=dag)

    for stream in flexport_streams["flexport_streams"]:
        # trigger meltano run
        trigger_meltano_run = SSHOperator(
            task_id=f"trigger_{stream['stream_name']}",
            ssh_conn_id="meltano_ec2_ssh",
            command='; '.join([
                "cd /home/ubuntu/prd-meltano-ruggable/flexport",
                "pyenv activate flexport-env",
                f"meltano schedule run flexport-{stream['stream_name']}"
            ]),
            sla=timedelta(minutes=60),
            cmd_timeout=28800,
            conn_timeout=28800,
            dag=dag,

        )

        start >> trigger_meltano_run >> end_ingest >> run_dbt_cloud_job >> end

