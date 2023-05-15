import glob


def task_parse_input_data():
    """
    Here we could parse/select which data we want to train on. Maybe not all files/rows are interesting.
    In the meantime let's just decide to train on latest.
    """
    data_for_clustering_dir = 'tests/ready_data_for_clustering/'
    return sorted(glob.glob(f"{data_for_clustering_dir}*.csv"), reverse=True)[0]