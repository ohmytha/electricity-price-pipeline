import os
import logging
import pandas as pd
from airflow.models import Variable

def extract_to_bronze():
    input_path = '/opt/airflow/datalake/master/electricity.csv'
    output_dir = '/opt/airflow/datalake/bronze/'

    df = pd.read_csv(input_path)
    df['date'] = pd.to_datetime(df['date'])

    last_date_str = Variable.get('bronze_last_date', default_var=None)
    if last_date_str is None:
        start_date = df['date'].min()
    else:
        start_date = pd.to_datetime(last_date_str)

    end_date = start_date + pd.Timedelta(days=7)

    filtered_df = df[(df['date'] >= start_date) & (df['date'] < end_date)]

    if filtered_df.empty:
        raise ValueError(f"No data found for week starting {start_date.date()}. All weeks may have been processed.")

    logging.info(f"Rows extracted for week {start_date.date()}: {len(filtered_df)}")

    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f'bronze_{start_date.date()}.csv')
    filtered_df.to_csv(output_path, index=False)

    Variable.set('bronze_last_date', str(end_date.date()))

    return output_path