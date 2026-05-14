import os
import logging
import pandas as pd

def extract_to_bronze(ds):
    input_path = '/opt/airflow/datalake/master/electricity.csv'
    output_dir = '/opt/airflow/datalake/bronze/'
    output_path = os.path.join(output_dir, f'bronze_{ds}.csv')
    
    df = pd.read_csv(input_path)
    df['date'] = pd.to_datetime(df['date'])
    
    start_date = pd.to_datetime(ds)
    end_date = start_date + pd.Timedelta(days=7)
    
    filtered_df = df[(df['date'] >= start_date) & (df['date'] < end_date)]
    
    if filtered_df.empty:
        raise ValueError("Filtered dataframe is completely empty.")
        
    logging.info(f"Rows extracted: {len(filtered_df)}")
    
    os.makedirs(output_dir, exist_ok=True)
    filtered_df.to_csv(output_path, index=False)
    
    return output_path