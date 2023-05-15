from typing import TypeVar

import dill
import glob
import pandas as pd

# magic to make the pandas inside the dill work
import __main__

__main__.pd = pd

output_dir = 'tests/sensor_features/'
TemperatureModel = TypeVar("TemperatureModel")  # of course we'll have the correct definition


def get_latest_feature_creator_model() -> TemperatureModel:
    feature_creator_models_dir = 'models/feature_creator/'
    latest = sorted(glob.glob(f"{feature_creator_models_dir}*.pkl"), reverse=True)[0]
    # Load the pre-trained model from the .pkl files
    with open(latest, 'rb') as f:
        feature_creator = dill.load(f)
    return feature_creator


def store_features(features_df: pd.DataFrame, filename: int):
    features_df.to_csv(f"{output_dir}/{filename}", index=False)


def task_create_features(**context):
    feature_creator = get_latest_feature_creator_model()
    # Test models
    conf = context["dag_run"].conf
    files_dir = conf['files_dir']
    sensor_id = conf['sensor_id']
    filename = f"{sensor_id}.csv"
    path = f"{files_dir}{filename}"
    time_series_df = pd.read_csv(path)
    train_features_df = feature_creator.transform(time_series_df)

    store_features(train_features_df, filename)
    return train_features_df
