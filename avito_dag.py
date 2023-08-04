from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from avito_etl import run_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 8),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'avito_dag',
    default_args=default_args,
    description='My first DAG with ETL process for Avito!',
    schedule_interval=timedelta(days=1),
)

etl = PythonOperator(
    task_id='complete_avito_etl',
    python_callable=run_etl, 
    op_args=["Your search here!"],  # put your search here as an arg!
    dag=dag, 
)

etl