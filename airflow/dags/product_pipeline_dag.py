from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='product_analytics_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    simulate_data = BashOperator(
        task_id='simulate_data',
        bash_command='python /opt/airflow/scripts/simulate_events.py'
    )

    ingest_data = BashOperator(
        task_id='ingest_to_snowflake',
        bash_command='python /opt/airflow/scripts/ingestion_to_snowflake.py /opt/airflow/.env'
    )

    bronze_to_silver = BashOperator(
        task_id='bronze_to_silver',
        bash_command='python /opt/airflow/scripts/bronze_to_silver/bronze_to_silver.py --env /opt/airflow/.env'
    )

    silver_to_gold = BashOperator(
        task_id='silver_to_gold',
        bash_command='python /opt/airflow/scripts/gold_aggregation/gold_aggregation.py --env /opt/airflow/.env'
    )

    simulate_data >> ingest_data >> bronze_to_silver >> silver_to_gold