from airflow import DAG
from airflow.operators.python import PythonOperator


from airflow.utils.dates import days_ago

from dags.retrain_model_tasks.parse_input_data import task_parse_input_data
from dags.retrain_model_tasks.input_validation import task_input_validation
from dags.retrain_model_tasks.retrain_model import task_retrain_model
from dags.retrain_model_tasks.log_metrics import task_log_metrics

with DAG(
    dag_id='retrain_model',
        start_date=days_ago(1),
        schedule_interval=None
) as dag:

    input_validation = PythonOperator(
        task_id='input_validation',
        python_callable=task_input_validation
    )

    parse_input_data = PythonOperator(
        task_id='parse_input_data',
        python_callable=task_parse_input_data
    )

    retrain_model = PythonOperator(
        task_id='retrain_model',
        python_callable=task_retrain_model
    )

    log_metrics = PythonOperator(
        task_id='log_metrics',
        python_callable=task_log_metrics
    )

input_validation >> parse_input_data >> retrain_model >> log_metrics
