from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import json

default_args = {
    'owner': 'data-engineering',
   'retries': 2,
   'retry_delay': timedelta(minutes=5),
    'email_on_failure': True
}

def on_failure_callback(context):
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    execution_date = context['execution_date']
    error_message = context['exception']
    logging.error(f"Dag: {dag_id}, Task: {task_id}, Execution Date: {execution_date}, Error: {error_message}")

def sla_miss_callback(context):
    dag_id = context['dag'].dag_id
    execution_date = context['execution_date']
    logging.error(f"Dag: {dag_id}, Execution Date: {execution_date}, SLA Miss")

def extract_bronze(**context):
    logging.info(f"Starting extract_bronze task: {context['task_instance']}")
    # Placeholder for Bronze layer extraction logic
    logging.info(f"Completed extract_bronze task: {context['task_instance']}")

def transform_silver(**context):
    logging.info(f"Starting transform_silver task: {context['task_instance']}")
    # Placeholder for Silver layer transformation logic
    logging.info(f"Completed transform_silver task: {context['task_instance']}")

def build_gold(**context):
    logging.info(f"Starting build_gold task: {context['task_instance']}")
    # Placeholder for Gold layer aggregation logic
    logging.info(f"Completed build_gold task: {context['task_instance']}")

with DAG(
    dag_id='sigma_transaction_pipeline',
    schedule='0 2 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    on_failure_callback=on_failure_callback,
    sla_miss_callback=sla_miss_callback,
    tags=['sigma', 'transactions', 'daily'],
    description="Daily Bronze->Silver->Gold pipeline for Sigma DataTech transactions"
) as dag:

    extract_bronze_task = PythonOperator(
        task_id='extract_bronze',
        python_callable=extract_bronze,
        on_failure_callback=on_failure_callback
    )

    transform_silver_task = PythonOperator(
        task_id='transform_silver',
        python_callable=transform_silver,
        on_failure_callback=on_failure_callback
    )

    build_gold_task = PythonOperator(
        task_id='build_gold',
        python_callable=build_gold,
        on_failure_callback=on_failure_callback
    )

    extract_bronze_task >> transform_silver_task >> build_gold_task
