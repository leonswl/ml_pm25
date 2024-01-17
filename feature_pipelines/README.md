# Feature Pipeline

This directory records data artifacts used in the pipeline for extracting data using APIs, perform feature engineering and loading the data into a feature store.


Fix required:
- feaure_pipeline_metadata.json 
  - requires `date_format`
  - requires `start_date` and `end_date` to be `start_datetime` and `end_datetime` instead
- extraction should be datetime instead of date, extracted to the latest **hour**