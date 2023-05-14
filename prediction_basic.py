import pandas as pd
import dill

feature_creator_model_file = 'models/feature_creator.pkl'
clustering_model_file = 'models/clustering_model.pkl'

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
clustering_model.train(train_features_df)  # TODO: this line breaks the code
predictions = clustering_model.inference(test_features_df)

print(predictions)
