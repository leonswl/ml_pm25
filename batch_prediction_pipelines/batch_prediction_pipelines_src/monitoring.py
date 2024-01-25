from typing import Optional

import hopsworks
import numpy as np
import pandas as pd

from sktime.performance_metrics.forecasting import mean_absolute_percentage_error

from batch_prediction_pipelines_src import load_batch_data
from batch_prediction_pipelines_src import settings
from batch_prediction_pipelines_src import utility


logger = utility.get_logger(__name__)


def compute(feature_view_version: Optional[int] = None) -> None:
    """Computes the metrics on the latest n_days of predictions.

    Args:
        feature_view_version: The version of the feature view to load data from the feature store. If None is provided, it will try to load it from the cached feature_view_metadata.json file.
    """

    if feature_view_version is None:
        feature_view_metadata = utility.load_json("feature_view_metadata.json")
        feature_view_version = feature_view_metadata["feature_view_version"]

    logger.info("Loading old predictions...")
    bucket = utility.get_bucket()
    predictions = utility.read_blob_from(
        bucket=bucket, blob_name=f"predictions_monitoring.parquet"
    )
    if predictions is None or len(predictions) == 0:
        logger.info(
            "Haven't found any predictions to compute the metrics on. Exiting..."
        )

        return
    logger.info("Successfully loaded old predictions.")

    predictions_min_timestamp = (
        predictions.index.min()
    )
    predictions_max_timestamp = (
        predictions.index.max()
    )
    logger.info(
        f"Loading predictions from {predictions_min_timestamp} to {predictions_max_timestamp}."
    )

    logger.info("Connecting to the feature store...")
    project = hopsworks.login(
        api_key_value=settings.SETTINGS["FS_API_KEY"],
        project=settings.SETTINGS["FS_PROJECT_NAME"],
    )
    fs = project.get_feature_store()
    logger.info("Successfully connected to the feature store.")

    

    logger.info("Loading latest data from feature store...")
    _, latest_observations = load_batch_data.load_data_from_feature_store(
        fs,
        feature_view_version,
        start_datetime=predictions_min_timestamp,
        end_datetime=predictions_max_timestamp,
    )
    logger.info("Successfully loaded latest data from feature store.")

    if len(latest_observations) == 0:
        logger.info(
            "Haven't found any new ground truths to compute the metrics on. Exiting..."
        )

        return

    logger.info("Computing metrics...")
    predictions = predictions.rename(
        columns={"reading_average": "reading_average_predictions"}
    )
    latest_observations = latest_observations.rename(
        columns={"reading_average": "reading_average_observations"}
    )

    predictions["reading_average_observations"] = np.nan
    predictions.update(latest_observations)

    # Compute metrics only on data points that have ground truth.
    predictions = predictions.dropna(subset=["reading_average_observations"])
    if len(predictions) == 0:
        logger.info(
            "Haven't found any new ground truths to compute the metrics on. Exiting..."
        )

        return

    mape_metrics = mean_absolute_percentage_error(
            predictions["reading_average_observations"],
            predictions["reading_average_predictions"],
            symmetric=False,
        )
    
    metrics = pd.Series(mape_metrics,name="MAPE").to_frame()

    logger.info("Successfully computed metrics...")

    logger.info("Saving new metrics...")
    utility.write_blob_to(
        bucket=bucket,
        blob_name=f"metrics_monitoring.parquet",
        data=metrics,
    )
    latest_observations = latest_observations.rename(
        columns={"reading_average_observations": "reading_average"}
    )
    utility.write_blob_to(
        bucket=bucket,
        blob_name=f"y_monitoring.parquet",
        data=latest_observations[["reading_average"]],
    )
    logger.info("Successfully saved new metrics.")


if __name__ == "__main__":
    compute()