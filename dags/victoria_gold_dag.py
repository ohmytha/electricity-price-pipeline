from datetime import datetime
from airflow.decorators import dag, task
from scripts.spark.gold_metrics import calculate_to_gold

default_args = {
    'retries': 1
}

@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2015, 1, 1),
    catchup=False
)
def victoria_gold_dag():

    @task
    def run_calculate_to_gold(**kwargs):
        import os
        silver_path = kwargs.get('dag_run').conf.get('silver_path')
        if not silver_path:
            silver_dir = '/opt/airflow/datalake/silver/'
            files = sorted(
                [f for f in os.listdir(silver_dir) if f.endswith('.parquet')],
                key=lambda f: os.path.getmtime(os.path.join(silver_dir, f)),
                reverse=True
            )
            if not files:
                raise FileNotFoundError("No silver parquet files found.")
            silver_path = os.path.join(silver_dir, files[0])
        filename = os.path.basename(silver_path)
        data_date = filename.replace('silver_', '').replace('.parquet', '')
        return calculate_to_gold(silver_path, data_date)

    run_calculate_to_gold()

victoria_gold_dag()
