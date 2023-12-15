from datetime import datetime
from typing import Optional

import hopsworks

from utility import get_logger, load_json, save_json
import settings
import hsfs

# set up logger
logger = get_logger(__name__)

def create(
    feature_group_version: Optional[int] = None
):
    """Create a new feature view version and training dataset
    based on the given feature group version and start and end datetimes.

    Args:
        feature_group_version (Optional[int]): The version of the
            feature group. If None is provided, it will try to load it
            from the cached feature_pipeline_metadata.json file.

    Returns:
        dict: The feature group version.

    """

    if feature_group_version is None:
        feature_pipeline_metadata = load_json("feature_pipeline_metadata.json")
        feature_group_version = feature_pipeline_metadata["feature_group_version"]

    project = hopsworks.login(
        api_key_value=settings.SETTINGS["FS_API_KEY"],
        project=settings.SETTINGS["FS_PROJECT_NAME"],
    )
    fs = project.get_feature_store()

    try:
        feature_views = fs.get_feature_views(name="pm25_singapore_view")
    except hsfs.client.exceptions.RestAPIError:
        # logger.info("No feature views found for pm25_singapore_view.")
        feature_views = []

    for feature_view in feature_views:
        try:
            feature_view.delete_all_training_datasets()
        except hsfs.client.exceptions.RestAPIError:
            logger.error(
                f"Failed to delete training datasets for feature view {feature_view.name} with version {feature_view.version}."
            )

        try:
            feature_view.delete()
        except hsfs.client.exceptions.RestAPIError:
            logger.error(
                f"Failed to delete feature view {feature_view.name} with version {feature_view.version}."
            )

    # Create feature view in the given feature group version.
    pm25_fg = fs.get_feature_group(
        "pm25_singapore", version=feature_group_version
    )
    ds_query = pm25_fg.select_all()
    feature_view = fs.create_feature_view(
        name="pm25_singapore_view",
        description="PM2.5 for Singapore forecasting model.",
        query=ds_query,
        labels=[],
    )

    # Create training dataset.
    # logger.info(
    #     f"Creating training dataset between {start_datetime} and {end_datetime}."
    # )
    feature_view.create_training_data(
        description="PM2.5 Singapore training dataset",
        data_format="csv",

        write_options={"wait_for_job": True},
        coalesce=False,
    )

    # Save metadata.
    metadata = {
        "feature_view_version": feature_view.version,
        "training_dataset_version": 1,
    }
    save_json(
        metadata,
        file_name="feature_view_metadata.json",
    )

    return metadata
