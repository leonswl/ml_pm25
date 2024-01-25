# Python script for loading data into feature store
import hopsworks
import pandas as pd
from great_expectations.core import ExpectationSuite
from hsfs.feature_group import FeatureGroup

from feature_pipelines_src.settings import SETTINGS

def to_feature_store(
    data: pd.DataFrame,
    validation_expectation_suite: ExpectationSuite,
    feature_group_version: int,
) -> FeatureGroup:
    """
    This function takes in a pandas DataFrame and a validation expectation suite,
    performs validation on the data using the suite, and then saves the data to a
    feature store in the feature store.

    Args:
        data [pandas dataframe]:
        validation_expectation_suite [ExpectationSuite]:
        feature_group_version [int]:

    Returns:

    """
    # Connect to feature store.
    project = hopsworks.login(
        api_key_value=SETTINGS["FS_API_KEY"], 
        project=SETTINGS["FS_PROJECT_NAME"]
    )
    feature_store = project.get_feature_store()

    # create feature group
    pm25_feature_group = feature_store.get_or_create_feature_group(
        name="pm25_singapore",
        version=feature_group_version,
        description="Singapore hourly PSI level data. ",
        primary_key=["timestamp"],
        event_time="timestamp",
        online_enabled=False,
        expectation_suite=validation_expectation_suite,
    )

    # Upload data.
    pm25_feature_group.insert(
        features=data,
        overwrite=False,
        write_options={
            "wait_for_job": True,
        },
    )

    # Add feature descriptions.
    feature_descriptions = [
        {
            "name": "timestamp",
            "description": """Data timestamp.""",
            "validation_rules": "Always full hours, i.e. minutes are 00",
        },
        {
            "name": "update_timestamp",
            "description": """Time of acquisition of data from NEA.""",
            "validation_rules": "Timestamp is complete to the seconds. E.g. '2023-12-08 01:03:52+08:00'",
        },
        {
            "name": "reading_west",
            "description": """PSI reading for west region.""",
            "validation_rules": ">0 (int), <500 (int)",
        },
        {
            "name": "reading_east",
            "description": "PSI reading for east region.",
            "validation_rules": ">=0 (float)",
        },
        {
            "name": "reading_central",
            "description": "PSI reading for central region.",
            "validation_rules": ">=0 (float)",
        },
        {
            "name": "reading_south",
            "description": "PSI reading for south region.",
            "validation_rules": ">=0 (float)",
        },
        {
            "name": "reading_north",
            "description": "PSI reading for north region.",
            "validation_rules": ">=0 (float)",
        },
        {
            "name": "reading_average",
            "description": "Average PSI reading for Singapore.",
            "validation_rules": ">=0 (float)",
        },
    ]
    
    for description in feature_descriptions:
        pm25_feature_group.update_feature_description(
            description["name"], description["description"]
        )
    # Update statistics.
    pm25_feature_group.statistics_config = {
        "enabled": True,
        "histograms": True,
        "correlations": True,
    }
    pm25_feature_group.update_statistics_config()
    pm25_feature_group.compute_statistics()

    return pm25_feature_group