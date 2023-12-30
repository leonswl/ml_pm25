# ml_pm25

This repository serves to illustrate and end-to-end deployment of a Machine Learning platform for batch prediction pipeline using Singapore PM2.5 open API data.




## 1. [Feature Pipelines](feature_pipelines)

The feature pipelines section focuses on leveraging APIs for extracting data, performing some feature engineering before loading them into a feature store (Hopswork).

**Usage**
```
# to run feature pipeline separately
python3 feature_pipelines/pipeline.py
```

**Summary**
1. Extract | Data is batch extracted using a real time API from [Data Gov](https://beta.data.gov.sg/collections/1394/datasets/d_9b2d180c92c4a3c45b5c671937bd1b5d/view).
2. Transform | The following transformations were applied to the data. More transformation maybe included as we iterate through the training process.
   - renaming columns
   - casting of column types
3. Validation | Build the data validation and integrity suite
4. Load | We will load the transformed data into Hopswork feature store.

## 2. [Training Pipelines](training_pipelines)

The training pipelines section will handle the heavylifting of model training. It will first pull the dataset from the feature store (Hopswork) and load the dataset metadata into wandb. The data will then be prepared for model training, with artifact outputs rendered and uploaded to wandb during each run.

**Usage**
```
# to run training pipelines separately
python3 training_pipelines/train.py
```

**Summary**
1. Load the data from Hopsworks.
   - [data.py](training_pipelines/data.py) will load the dataset using `load_dataset_from_feature_store()` from the Hopsworks feature store with the given version of feature view and training data.   
2. Initialize a W&B run.  
   - [data.py](training_pipelines/data.py) will then log to W&B the metadata related to this dataset.
   - The data is prepared for sktime, which requires the data to be modeled using multi-indexes. The `prepare_data()` function will set the timestamp as PeriodIndexes and split the data into train and test set.  
3. Load the best_config artifact.
4. Build the baseline model.
   - [models.py](training_pipelines/models.py) will build a naive baseline model using `build_baseline_model.py`.
5. Train and evaluate the baseline model on the test split.
   - [train.py](training_pipelines/train.py) will train and evaluate the baseline model using `train_model()` and `evaluate()`. 
6. Build the fancy model using the latest best configuration.
   - [models.py](training_pipelines/models.py) will build a fancy model using Sktome and LightGBM using `buid_model()`.
7. Train and evaluate the fancy model on the test split.
   - [train.py](training_pipelines/train.py) will train and evaluate the Sktime and lightGBM models using `train_model()` and `evaluate()`. 
8. Render the results to see how they perform visually.
   - [train.py](training_pipelines/train.py) will render the visuals
9.  Retrain the model on the whole dataset. This is critical for time series models as you must retrain them until the present moment to forecast the future.
10.  Forecast future values.
11.  Render the forecasted values.
12.  Save the best model as an Artifact in W&B and the best model in the Hopsworks' model registry