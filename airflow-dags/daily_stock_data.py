# Use airflow to schedule a number of tasks to complete a regular, automated analysis of daily stock data.

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, date

today = date.today()

# Create a PythonOperator to download the market data (python_download_aapl, python_download_tsla)
def download_data(stock):
    import yfinance as yf
        
    start_date = date.today()
    end_date = start_date + timedelta(days=1)
    df = yf.download(stock, start=start_date, end=end_date, interval='1m')
    df.to_csv(f"{stock}_data.csv", header=True)

def stock_volume():
    '''Return query for volume from a particular file.'''
    import pandas as pd

    aapl_df = pd.read_csv(f'/tmp/data/{str(today)}/aapl_data.csv')
    tsla_df = pd.read_csv(f'/tmp/data/{str(today)}/tsla_data.csv')
    query = [aapl_df['Volume'][0], tsla_df['Volume'][0]]
    return query

# Create Airflow DAG
default_args = {
    "retries": 2,
    'retry_delay': timedelta(minutes=5)
}

move_file_command="""
    mv /opt/airflow/{{ params.csv_name }} {{ params.final_dest }}
"""

with DAG(
    dag_id='marketvol',
    default_args=default_args,
    description='A simple DAG',
    schedule_interval='0 18 * * 1-5',
    start_date=datetime(2021, 12, 6, hour=18),
    catchup=False
) as dag:

    # Create a BashOperator to initialize a temporary directory for data download
    t0 = BashOperator(
        task_id='init_temp_directory',
        bash_command=f'mkdir -p /tmp/data/{str(today)}'
    )

    # Create PythonOperator to download the data to a csv
    t1 = PythonOperator(
        task_id='download_aapl',
        python_callable=download_data,
        op_kwargs={'stock':'aapl'}
    )

    t2 = PythonOperator(
        task_id='download_tsla',
        python_callable=download_data,
        op_kwargs={'stock':'tsla'}
    )

    # Create BashOperator to move the downloaded file to a data location
    t3 = BashOperator(
        task_id='move_aapl',
        bash_command=move_file_command,
        params={'csv_name': 'aapl_data.csv', 'final_dest':f'/tmp/data/{str(today)}'}
    )

    t4 = BashOperator(
        task_id='move_tsla',
        bash_command=move_file_command,
        params={'csv_name': '/tsla_data.csv', 'final_dest':f'/tmp/data/{str(today)}'}
    )

    # Create a PythonOperator to run a query on both data files in the specified location
    t5 = PythonOperator(
        task_id='show_stock_volume',
        python_callable=stock_volume
    )

# Set job dependencies
t0 >> [t1, t2]
t1 >> t3
t2 >> t4
[t3, t4] >> t5