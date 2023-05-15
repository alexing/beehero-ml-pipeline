from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.utils.dates import days_ago

from dags.feature_creation_tasks.input_validation import task_input_validation
from dags.feature_creation_tasks.create_features import task_create_features




default_args = {
    'email': ['hi@alexingberg.com'],
    'email_on_failure': False,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='feature_creation',
    start_date=days_ago(1),
    schedule_interval="0 16 * * *",  # daily @ 16:00hs
    default_args=default_args,
    max_active_runs=120
) as dag:

    input_validation = PythonOperator(
        task_id='input_validation',
        python_callable=task_input_validation
    )

    create_features = PythonOperator(
        task_id='create_features',
        python_callable=task_create_features
    )

input_validation >> create_features
