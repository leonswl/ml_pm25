"""Python script to execute a run for the ETL pipeline"""
import fire
import pandas as pd

import extract, transform, validation, load
from utility import get_logger, save_json

import settings

# set up logging
logger = get_logger(__name__)

def run(
    feature_group_version: int = 1,    
):

    logger.info(f"Extracting data from API.")

    url = "https://api.data.gov.sg/v1/environment/pm25"

    data, metadata = extract.extract(url,feature_group_version)

    logger.info("Successfully extracted data from API.")

    logger.info(f"Transforming data.")
    data = transform.transform(data)
    logger.info("Successfully transformed data.")

    logger.info("Building validation expectation suite.")
    validation_expectation_suite = validation.build_expectation_suite()
    logger.info("Successfully built validation expectation suite.")

    logger.info(f"Validating data and loading it to the feature store.")
    load.to_feature_store(
        data,
        validation_expectation_suite=validation_expectation_suite,
        feature_group_version=feature_group_version,
    )

    # metadata["feature_group_version"] = feature_group_version
    logger.info("Successfully validated data and loaded it to the feature store.")

    logger.info(f"Wrapping up the pipeline.")

    logger.info(f"Caching data and metadata")
    # cache data
    logger.info(f"metadata contains: {metadata}")
    data.to_parquet(f"{metadata['cache_dir']}/PM25_Hourly.parquet",index=False)
    save_json(metadata, file_name="feature_pipeline_metadata.json", save_dir=f"{settings.OUTPUT_DIR}") # export metadata
    logger.info("Done!")

if __name__ == "__main__":
    fire.Fire(run)