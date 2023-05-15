from airflow import DAG
from airflow.operators.python import PythonOperator


from airflow.utils.dates import days_ago

from dags.clustering_inference_tasks.infer_predictions import task_infer_predictions
from dags.clustering_inference_tasks.log_metrics import task_log_metrics
from dags.clustering_inference_tasks.log_predictions import task_log_predictions
from dags.clustering_inference_tasks.parse_input_data import task_parse_input_data
from dags.feature_creation_tasks.input_validation import task_input_validation

with DAG(
    dag_id='clustering_inference',
        start_date=days_ago(1),
        schedule_interval="0 21 * * *",  # daily @ 21:00hs
) as dag:

    input_validation = PythonOperator(
        task_id='input_validation',
        python_callable=task_input_validation
    )

    parse_input_data = PythonOperator(
        task_id='parse_input_data',
        python_callable=task_parse_input_data
    )

    infer_predictions = PythonOperator(
        task_id='infer_predictions',
        python_callable=task_infer_predictions
    )

    log_predictions = PythonOperator(
        task_id='log_predictions',
        python_callable=task_log_predictions
    )

    log_metrics = PythonOperator(
        task_id='log_metrics',
        python_callable=task_log_metrics
    )

input_validation >> parse_input_data >> infer_predictions >> log_predictions >> log_metrics
