from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.models import DagRun, TaskInstance
from airflow.utils.state import State
from airflow.utils.db import create_session

dag_id = 'shopify_graphql_order_custom_attributes'
start_date = datetime(2016,1,1,tzinfo=timezone.utc)
end_date = datetime(2024,9,1,tzinfo=timezone.utc)

@dag(
    dag_id='clear_dag_runs',
    schedule='@once',
    start_date=datetime(2016,1,1,tzinfo=timezone.utc),
    tags=['graphql', 'shopify'],
    catchup=False,
    default_args={
        "owner": "airflow",
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 3,
        "max_retry_delay": 60,
    },
    dagrun_timeout = timedelta(minutes=20),
    max_active_runs=1,
)
def clear_dag_runs():

    @task
    def clear_state():
        with create_session() as session:
            dag_runs = session.query(DagRun).filter(
                DagRun.dag_id == dag_id,
                DagRun.execution_date >= start_date,
                DagRun.execution_date <= end_date,
                DagRun.state == State.FAILED,
            ).all()

            for dag_run in dag_runs:
                task_instances = dag_run.get_task_instances(session=session)
                for ti in task_instances:
                    if ti.state == State.FAILED:
                        ti.set_state(State.NONE, session=session)

        # This script clears all failed tasks in failed DAG runs so that they can be retried.
    
    clear_state()

clear_dag_runs()