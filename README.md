# ml_pm25

This repository serves to illustrate and end-to-end deployment of a Machine Learning platform for batch prediction pipeline using Singapore PM2.5 open API data.




## Feature Pipelines

```
python3 feature_pipelines/pipeline.py

```

### Extract

Data is batch extracted using a real time API from [Data Gov](https://beta.data.gov.sg/collections/1394/datasets/d_9b2d180c92c4a3c45b5c671937bd1b5d/view).

### Transform
The following transformations were applied to the data. More transformation maybe included as we iterate through the training process.
- renaming columns
- casting of column types

### Validation
Build the data validation and integrity suite

### Load
We will load the transformed data into Hopswork feature store.

## Training Pipelines

This sequence of steps for the training pipeline can be summarised below:
1. Load the data from Hopsworks.
2. Initialize a W&B run.
3. Load the best_config artifact.
4. Build the baseline model.
5. Train and evaluate the baseline model on the test split.
6. Build the fancy model using the latest best configuration.
7. Train and evaluate the fancy model on the test split.
8. Render the results to see how they perform visually.
9. Retrain the model on the whole dataset. This is critical for time series models as you must retrain them until the present moment to forecast the future.
10. Forecast future values.
11. Render the forecasted values.
12. Save the best model as an Artifact in W&B
13. Save the best model in the Hopsworks' model registry