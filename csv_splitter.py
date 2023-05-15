import pandas as pd
train_dataset_file = 'datasets/train.csv'
output_dir = f'datasets/split/'
train_df = pd.read_csv(train_dataset_file)
for sensor_id in train_df.sensor_id.unique():
    df_chunk = train_df[train_df.sensor_id == sensor_id]
    filename = f'{sensor_id}.csv'
    df_chunk.to_csv(f'{output_dir}{filename}', index=False)
    print(f'dumped {filename}')
