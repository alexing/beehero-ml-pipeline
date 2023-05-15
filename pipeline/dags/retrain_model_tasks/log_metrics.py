import datetime as dt

log_dir = 'tests/logs/metrics_db.txt'


def log(row: str) -> None:
    with open(log_dir, 'a') as f:
        f.write(row)


def task_log_metrics(**context):
    metrics = context['task_instance'].xcom_pull(key='metrics')
    run_id = context['dag_run'].run_id
    start_date = context['dag_run'].start_date
    now = dt.datetime.now(tz=dt.timezone.utc)

    duration = now - start_date
    log(f"Metrics for TRAINING run_id: {run_id} -> silhouette_score: {metrics['silhouette_score']}, "
        f"calinski_harabasz_score: {metrics['calinski_harabasz_score']}, "
        f"davies_bouldin_score: {metrics['davies_bouldin_score']}, "
        f"inference duration: {duration.microseconds / 1e6} seconds\n")
