from airflow.api.client.local_client import Client

c = Client(None, None)

files_dir = "tests/sensor_timeseries/"
sensor_id = 23241
c.trigger_dag(dag_id='feature_creation', run_id='test_run_id', conf={"files_dir": files_dir, "sensor_id": sensor_id})

# {"files_dir":  "tests/sensor_timeseries/", "sensor_id": 23241}


c.trigger_dag(dag_id='clustering_inference', run_id='test_run_id', conf={"features_dir": "tests/sensor_features/"})
