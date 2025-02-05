from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from user_operator import RandomUserOperator


start_date = datetime(2025,2,3)

with DAG(
    dag_id='custom_ops_id',
    default_args={
        "depends_on_past": False,
        "owner": "maodo",
        "backfill": False
    },
    schedule_interval='@daily',
    start_date=start_date,
    description='My DAG for learning airflow extension'
) as dag:
    
    start = EmptyOperator(task_id='start')

    end = EmptyOperator(task_id='end')

    fetch_and_save_users = RandomUserOperator(task_id='fetch_users')

    start >> fetch_and_save_users >> end