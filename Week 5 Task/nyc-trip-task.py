from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args={
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    'dbt_nyc_taxi_trip',
    default_args=default_args,
    description='Week 5 Assignment DBT with Airflow',
    schedule_interval=None,
    start_date=datetime(2023, 10, 24),
    tags=['DF11-Week 5'],   
) as dag:
    
    ## Get the raw data into raw.nyc_trip_data task
    
    dbt_version_check = BashOperator(
        task_id="dbt_version_check",
        bash_command="dbt --version",
    )

    dbt_run_task = BashOperator(
        task_id="dbt_run_task",
        bash_command= "cd /opt/airflow/dags/dbts/nyc_taxi_trip && dbt run --full-refresh --profiles-dir ."
    )

dbt_version_check >> dbt_run_task