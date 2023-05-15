import glob
from datetime import datetime
from typing import TypeVar
import sklearn

import dill
import pandas as pd

# magic to make the pandas inside the dill work
import __main__
__main__.pd = pd

output_dir = 'tests/predictions/'

SensorCluster = TypeVar("SensorCluster")  # of course we'll have the correct definition


def get_latest_clustering_model() -> SensorCluster:
    clustering_models_dir = 'models/clustering/'
    latest = sorted(glob.glob(f"{clustering_models_dir}*.pkl"), reverse=True)[0]
    # Load the pre-trained model from the .pkl files
    with open(latest, 'rb') as f:
        clustering_model = dill.load(f)
    return clustering_model


def store_features(predictions: pd.DataFrame) -> str:
    now = datetime.today().strftime('%Y-%m-%dT%H:%M:%S')
    path = f"{output_dir}/{now}.csv"
    predictions.to_csv(path, index=False)
    return path


def task_infer_predictions(**context):
    clustering_model = get_latest_clustering_model()
    # Test models
    path_to_df = context['task_instance'].xcom_pull(task_ids='parse_input_data')
    clustering_df = pd.read_csv(path_to_df)
    predictions = clustering_model.inference(clustering_df)

    path = store_features(predictions)

    X = clustering_df[['daily_mean', 'daily_std']]
    metrics = {
        'silhouette_score': sklearn.metrics.silhouette_score(X, predictions),
        'calinski_harabasz_score': sklearn.metrics.calinski_harabasz_score(X, predictions),
        'davies_bouldin_score': sklearn.metrics.davies_bouldin_score(X, predictions)
    }
    context['task_instance'].xcom_push(key='metrics', value=metrics)
    return path
