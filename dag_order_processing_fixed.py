from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import random

default_args = {
    'owner': 'data_eng',
    'start_date': datetime(2023, 1, 1),
    'depends_on_past': False, 
    'retries': 4, 
    'retry_delay': timedelta(minutes=4), 
}

dag = DAG(
    dag_id='order_processing_extended', 
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False, 
    tags=['orders'],
    max_active_runs=1, 
    on_failure_callback=notify_failure 
)

def extract_orders(**context):
    print("Extracting orders...")
    if random.random() < 0.2:
        raise Exception("Random extraction failure") 

def transform_orders(**context):
    print("Transforming orders...")

def load_orders(**context):
    print("Loading orders to warehouse...")

def notify_failure(**context): 
    print("Sending alert to Slack...")  

def check_orders_not_empty():
    hook = PostgresHook(postgres_conn_id='postgresELMIR')
    result = hook.get_first("SELECT COUNT(*) FROM orders_agg;")

    if result[0] == 0:
        raise ValueError("Таблица пуста\(")

start = EmptyOperator(task_id='start', dag=dag)

extract = PythonOperator(
    task_id='extract_orders',
    python_callable=extract_orders,
    dag=dag,
)

transform = PythonOperator(
    task_id='transform_orders',
    python_callable=transform_orders,
    dag=dag,
)

load = PythonOperator(
    task_id='load_orders',
    python_callable=load_orders,
    dag=dag,
)

check_not_empty = PythonOperator(
    task_id='check_agg_table',
    python_callable=check_orders_not_empty,
    dag=dag,
)

end = EmptyOperator(task_id='end', dag=dag)

start >> extract >> transform >> load >> check_not_empty >> end