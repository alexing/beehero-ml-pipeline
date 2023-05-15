import pandas as pd
import dill

feature_creator_model_file = 'pipeline/models/feature_creator/2023-05-14T08:00:00.pkl'
clustering_model_file = 'pipeline/models/clustering/2023-05-15T08:00:00.pkl'

train_dataset_file = 'datasets/train.csv'
test_dataset_file = 'datasets/test.csv'

# Load the pre-trained models from the .pkl files
with open(feature_creator_model_file, 'rb') as f:
    feature_creator = dill.load(f)
with open(clustering_model_file, 'rb') as f:
    clustering_model = dill.load(f)

# Load the datasets
train_df = pd.read_csv(train_dataset_file)
test_df = pd.read_csv(test_dataset_file)


# Test models
train_features_df: pd.DataFrame = feature_creator.transform(train_df)
test_features_df: pd.DataFrame = feature_creator.transform(test_df)
clustering_model.train(train_features_df)
predictions = clustering_model.inference(test_features_df)

print(predictions)
