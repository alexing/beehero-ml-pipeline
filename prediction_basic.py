import pandas as pd
import dill
import sklearn

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


# metrics
silhouette_score = sklearn.metrics.silhouette_score(test_features_df[['daily_mean', 'daily_std']], predictions)
print(f"silhouette_score: {silhouette_score}")
calinski_harabasz_score = sklearn.metrics.calinski_harabasz_score(test_features_df[['daily_mean', 'daily_std']], predictions)
print(f"calinski_harabasz_score: {calinski_harabasz_score}")
davies_bouldin_score = sklearn.metrics.davies_bouldin_score(test_features_df[['daily_mean', 'daily_std']], predictions)
print(f"davies_bouldin_score: {davies_bouldin_score}")

