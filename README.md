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
