from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from scripts.extract_energy import extract_to_bronze

default_args = {
    'retries': 1
}

@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2015, 1, 1),
    catchup=False
)
def victoria_bronze_dag():

    @task
    def run_extract_to_bronze():
        return extract_to_bronze()

    trigger_silver = TriggerDagRunOperator(
        task_id='trigger_victoria_silver_dag',
        trigger_dag_id='victoria_silver_dag',
        conf={"bronze_path": "{{ task_instance.xcom_pull(task_ids='run_extract_to_bronze') }}"},
        execution_date='{{ execution_date }}'
    )

    run_extract_to_bronze() >> trigger_silver

victoria_bronze_dag()