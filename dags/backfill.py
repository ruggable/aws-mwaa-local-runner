from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowNotFoundException

def check_backfill_flag(**kwargs):
    # Check the 'run_backfill' parameter from the DagRun configuration
    ti = kwargs["ti"]
    dag_id = kwargs['dag_run'].conf.get('dag_id')
    start = kwargs['dag_run'].conf.get('start')
    end = kwargs['dag_run'].conf.get('end')
    if not dag_id:
        raise AirflowNotFoundException('Unable to locate a dag_id to backfill in the config.')
    if not start:
        raise AirflowNotFoundException('Unable to locate a start timestamp for backfill in the config.')
    if not end:
        raise AirflowNotFoundException('Unable to locate a end timestamp for backfill in the config.')
    ti.xcom_push("backfill_dag_id", dag_id)
    ti.xcom_push("backfill_start", start)
    ti.xcom_push("backfill_end", end)
    

with DAG(
    dag_id="backfill", 
    schedule_interval=None, 
    catchup=False, 
    start_date=days_ago(1),
    dagrun_timeout=timedelta(days=7)
    ) as dag:
    
    # Decide whether to backfill based on the parameter
    get_backfill_dag = PythonOperator(
        task_id="get_backfill_dag",
        python_callable=check_backfill_flag,
        provide_context=True
    )

    # Backfill task
    run_backfill_task = BashOperator(
        task_id="run_backfill",
        bash_command='airflow dags backfill -B -s "{{ task_instance.xcom_pull(task_ids="get_backfill_dag", key="backfill_start")}}" -e "{{ task_instance.xcom_pull(task_ids="get_backfill_dag", key="backfill_end")}}" {{ task_instance.xcom_pull(task_ids="get_backfill_dag", key="backfill_dag_id")}}',
        trigger_rule=TriggerRule.ALL_SUCCESS 
    )
    
    # Set the dependencies
    get_backfill_dag >> run_backfill_task
