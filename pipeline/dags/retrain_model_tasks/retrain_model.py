from datetime import datetime
from typing import TypeVar
import glob
import dill
import pandas as pd
import sklearn

# magic to make the pandas inside the dill work
import __main__
__main__.pd = pd

SensorCluster = TypeVar("SensorCluster")  # of course we'll have the correct definition
clustering_models_dir = 'models/clustering/'


def get_latest_clustering_model() -> SensorCluster:
    latest = sorted(glob.glob(f"{clustering_models_dir}*.pkl"), reverse=True)[0]
    # Load the pre-trained model from the .pkl files
    with open(latest, 'rb') as f:
        clustering_model = dill.load(f)
    return clustering_model


def store_model(clustering_model: SensorCluster) -> None:
    now = datetime.today().strftime('%Y-%m-%dT%H:%M:%S')
    filename = f"{clustering_models_dir}{now}.pkl"
    with open(filename, 'wb') as f:
        dill.dump(clustering_model, f)


def push_metrics(clustering_model: SensorCluster, train_df: pd.DataFrame, **context):
    predictions = clustering_model.inference(train_df)
    X = train_df[['daily_mean', 'daily_std']]
    metrics = {
        'silhouette_score': sklearn.metrics.silhouette_score(X, predictions),
        'calinski_harabasz_score': sklearn.metrics.calinski_harabasz_score(X, predictions),
        'davies_bouldin_score': sklearn.metrics.davies_bouldin_score(X, predictions)
    }
    context['task_instance'].xcom_push(key='metrics', value=metrics)


def task_retrain_model(**context):
    path_to_df = context['task_instance'].xcom_pull(task_ids='parse_input_data')
    clustering_df = pd.read_csv(path_to_df)
    clustering_model = get_latest_clustering_model()
    clustering_model.train(clustering_df)

    push_metrics(clustering_model, clustering_df, **context)

    store_model(clustering_model)
