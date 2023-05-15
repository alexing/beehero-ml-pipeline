import glob
from datetime import datetime
from time import sleep
from typing import Dict

from airflow_api import AirflowAPIService

airflow = AirflowAPIService()
now = datetime.today().strftime('%Y-%m-%dT%H:%M:%S')
FEATURE_CREATION_DAG_ID = 'feature_creation'
CLUSTERING_INFERENCE_DAG_ID = 'clustering_inference'
RETRAIN_MODEL_DAG_ID = 'retrain_model'


def activate_dags() -> None:
    dags = airflow.get_dags()
    for a_dag_id in [dag['dag_id'] for dag in dags['dags'] if dag['is_paused']]:
        print(f'unpausing {a_dag_id}')
        airflow.unpause_dags(a_dag_id)


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


def wait_for_feature_creation_dags_completion(feature_creation_dags_done: Dict[str, bool]) -> None:
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


def launch_clustering_dag() -> str:
    # trigger clustering
    dag_run_id = f"clustering_{now}"
    print(f'triggering {dag_run_id}')
    airflow.trigger_dag_run(CLUSTERING_INFERENCE_DAG_ID, dag_run_id,
                            conf={"features_dir": "tests/sensor_features/"})
    return dag_run_id


def launch_retrain_dag() -> None:
    dag_run_id = f"training_{now}"
    print(f'triggering {dag_run_id}')
    airflow.trigger_dag_run(RETRAIN_MODEL_DAG_ID, dag_run_id)


def wait_for_clustering_dag_completion(dag_run_id: str) -> None:
    timeout = 20  # 20 seconds
    iterations = 0
    clustering_done = False
    while not clustering_done:

        dag_run = airflow.get_dag_run(CLUSTERING_INFERENCE_DAG_ID, dag_run_id)
        if dag_run['state'] == 'success':
            print(f"{dag_run_id} succeeded!")
            clustering_done = True
        sleep(1)
        iterations += 1
        if iterations >= timeout:
            raise Exception('something is wrong')


def main():
    activate_dags()
    feature_creation_dags = launch_feature_creation_dags()
    wait_for_feature_creation_dags_completion(feature_creation_dags)
    clustering_dag_run_id = launch_clustering_dag()

    # we will retrain as soon as the clustering is done. This could be improved in the future
    wait_for_clustering_dag_completion(clustering_dag_run_id)
    launch_retrain_dag()


if __name__ == "__main__":
    main()
