from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from scripts.spark.clean_energy import clean_to_silver

default_args = {
    'retries': 1
}

@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2015, 1, 1),
    catchup=False
)
def victoria_silver_dag():

    @task
    def run_clean_to_silver(**kwargs):
        import os
        bronze_path = kwargs.get('dag_run').conf.get('bronze_path')
        if not bronze_path:
            bronze_dir = '/opt/airflow/datalake/bronze/'
            files = sorted(
                [f for f in os.listdir(bronze_dir) if f.endswith('.csv')],
                key=lambda f: os.path.getmtime(os.path.join(bronze_dir, f)),
                reverse=True
            )
            if not files:
                raise FileNotFoundError("No bronze CSV files found.")
            bronze_path = os.path.join(bronze_dir, files[0])
        filename = os.path.basename(bronze_path)
        data_date = filename.replace('bronze_', '').replace('.csv', '')
        return clean_to_silver(bronze_path, data_date)

    trigger_gold = TriggerDagRunOperator(
        task_id='trigger_victoria_gold_dag',
        trigger_dag_id='victoria_gold_dag',
        conf={"silver_path": "{{ task_instance.xcom_pull(task_ids='run_clean_to_silver') }}"},
        execution_date='{{ execution_date }}'
    )

    run_clean_to_silver() >> trigger_gold

victoria_silver_dag()
