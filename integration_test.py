import glob
from datetime import datetime
from time import sleep
from typing import Dict

from airflow_api import AirflowAPIService

airflow = AirflowAPIService()
now = datetime.today().strftime('%Y-%m-%dT%H:%M:%S')
FEATURE_CREATION_DAG_ID = 'feature_creation'
CLUSTERING_INFERENCE_DAG_ID = 'clustering_inference'


def activate_dags():
    dags = airflow.get_dags()
    for a_dag_id in [dag['dag_id'] for dag in dags['dags'] if dag['is_paused']]:
        print(f'unpausing {a_dag_id}')
        print(airflow.unpause_dags(a_dag_id))


def launch_feature_creation_dags() -> Dict[str, bool]:
    timeseries_files_dir = "pipeline/tests/sensor_timeseries/"
    sensor_timeseries = glob.glob(f"{timeseries_files_dir}*.csv")

    feature_creation_dag_runs_done = {}

    # trigger feature creation
    for a_sensor_file in sensor_timeseries:
        sensor_id = a_sensor_file.replace(timeseries_files_dir, "").replace(".csv", "")
        dag_run_id = f"sensor_{sensor_id}_{now}"
        print(f'triggering {dag_run_id}')
        dag_run = airflow.trigger_dag_run(FEATURE_CREATION_DAG_ID, dag_run_id, conf={
            "files_dir": "tests/sensor_timeseries/", "sensor_id": sensor_id
        })
        feature_creation_dag_runs_done[dag_run_id] = False
    return feature_creation_dag_runs_done


def wait_for_feature_creation_dags_completion(feature_creation_dags_done: Dict[str, bool]):
    timeout = 20  # 20 seconds
    iterations = 0
    while not all(feature_creation_dags_done.values()):
        for dag_run_id in feature_creation_dags_done:
            dag_run = airflow.get_dag_run(FEATURE_CREATION_DAG_ID, dag_run_id)
            if dag_run['state'] == 'success':
                print(f"{dag_run_id} succeeded!")
                feature_creation_dags_done[dag_run_id] = True
            sleep(1)
        iterations += 1
        if iterations >= timeout:
            raise Exception('something is wrong')


def launch_clustering_dag():
    # trigger clustering
    dag_run_id = f"clustering_{now}"
    print(f'triggering {dag_run_id}')
    dag_run = airflow.trigger_dag_run(CLUSTERING_INFERENCE_DAG_ID, dag_run_id,
                                      conf={"features_dir": "tests/sensor_features/"})


def main():
    activate_dags()
    feature_creation_dags = launch_feature_creation_dags()
    wait_for_feature_creation_dags_completion(feature_creation_dags)
    launch_clustering_dag()


if __name__ == "__main__":
    main()
