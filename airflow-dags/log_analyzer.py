import logging
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator

logging.basicConfig(level=logging.WARNING)

# The folder where airflow should store its log files.
base_log_folder = '/opt/airflow/logs/marketvol'

def analyze_file(stock, log_dir):
    '''
    Input: a log file
    Output:
    - The total count of error entries from this file
    - A list of error message details (the errors themself)
    '''
    # Find all the error messages in log to report the total number of errors and their associated error messages.
    from pathlib import Path

    file_list = Path(log_dir).rglob('*.log')
    file_list = [str(f) for f in file_list]
    error_count = 0
    error_list = []

    for file in file_list:
        with open(file, "r") as f:
            error_list = [line.replace('\n', '') for line in f if 'ERROR' in line]
        
    error_count = len(error_list)
    logging.info(f'Total count is: {error_count}')
    logging.info(f'Error list is : {error_list}')
    
    return error_count, error_list

# Create Airflow dag
default_args = {
    "retries": 2,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='loganalyzer',
    default_args=default_args,
    description='A simple DAG',
    schedule_interval='0 18 * * 1-5',
    start_date=datetime(2021, 11, 20, hour=18),
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id='aapl_log_errors',
        python_callable=analyze_file,
        op_kwargs={'stock': 'aapl', 'log_dir': base_log_folder}
    )

    t2 = PythonOperator(
        task_id='tsla_log_errors',
        python_callable=analyze_file,
        op_kwargs={'stock': 'tsla', 'log_dir': base_log_folder}
    )

t1 >> t2