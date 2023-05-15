from datetime import datetime
import pandas as pd

log_dir = 'tests/logs/results_db.txt'


def log(row: str) -> None:
    with open(log_dir, 'a') as f:
        f.write(row)


def task_log_predictions(**context):
    # Test models
    predictions_path = context['task_instance'].xcom_pull(task_ids='infer_predictions')
    input_file_path = context['task_instance'].xcom_pull(task_ids='parse_input_data')
    predictions = pd.read_csv(predictions_path)
    now = datetime.today().strftime('%Y-%m-%dT%H:%M:%S')
    parsed_clusters = [int(str(label).strip()) for label in predictions['cluster_labels'].values]
    log(f"Predictions for {now} (file: {input_file_path}) are: {parsed_clusters}\n")
