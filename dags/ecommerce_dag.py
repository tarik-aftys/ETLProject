from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'data_engineer_student',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='ecommerce_end_to_end_pipeline',
    default_args=default_args,
    description='Pipeline complet (Ingestion -> dbt -> Tests)',
    schedule_interval='@daily',
    catchup=False,
    tags=['ecommerce', 'elt'],
) as dag:

    ingest_csv = BashOperator(
        task_id='ingest_csv_data',
        bash_command='python /opt/airflow/scripts/ingest_csv.py',
    )
    
    ingest_api = BashOperator(
        task_id='ingest_api_data',
        bash_command='python /opt/airflow/scripts/ingest_api.py',
    )

    ingest_scraping = BashOperator(
        task_id='ingest_wikipedia_data',
        bash_command='python /opt/airflow/scripts/ingest_scraping.py',
    )

    dbt_run = BashOperator(
        task_id='dbt_transform_models',
        bash_command='cd /opt/airflow/ecommerce_dbt && dbt run --profiles-dir . --target-path /tmp/dbt_target --log-path /tmp/dbt_logs',
    )
