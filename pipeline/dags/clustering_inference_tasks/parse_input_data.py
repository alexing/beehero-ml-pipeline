import pandas as pd
import glob
import os
from datetime import datetime

output_dir = 'tests/ready_data_for_clustering/'


def store_features(features_df: pd.DataFrame) -> str:
    today = datetime.today().strftime('%Y-%m-%d')
    path = f"{output_dir}/{today}.csv"
    features_df.to_csv(path, index=False)
    return path


def task_parse_input_data(**context) -> str:
    # Test models
    conf = context["dag_run"].conf
    files_dir = conf['features_dir']
    all_files = glob.glob(os.path.join(files_dir, "*.csv"))

    list_of_dfs = []

    for filename in all_files:
        df = pd.read_csv(filename)
        list_of_dfs.append(df)

    df_to_cluster = pd.concat(list_of_dfs, axis=0, ignore_index=True)

    path = store_features(df_to_cluster)
    return path
