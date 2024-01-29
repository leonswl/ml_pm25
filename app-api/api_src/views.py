from typing import Any

import pandas as pd
from fastapi import APIRouter, HTTPException

from api_src import schemas
from api_src.config import get_settings

from api_src.utility import get_bucket, read_blob_from

# get GCS bucket
bucket = get_bucket()

api_router = APIRouter()

@api_router.get(
        "/health", 
        response_model=schemas.Health, 
        status_code=200)
def health() -> dict:
    """
    Health check endpoint.
    """

    health_data = schemas.Health(
        name=get_settings().PROJECT_NAME, api_version=get_settings().VERSION
    )

    return health_data.dict()

@api_router.get(
    "/predictions",
    response_model=schemas.PredictionResults,
    status_code=200,
)
async def get_predictions() -> Any:
    """
    Get forecasted predictions
    """

    # Download the data from GCS.
    train_df = read_blob_from(
        bucket=bucket, blob_name=f"y.parquet"
    )
    preds_df = read_blob_from(
        bucket=bucket, blob_name=f"predictions.parquet"
    )

    if len(train_df) == 0 or len(preds_df) == 0:
        raise HTTPException(
            status_code=404,
            detail=f"No data found"
        )

    # Return only the latest week of observations.
    train_df = train_df.sort_index().tail(24 * 7)

    # Prepare data to be returned.
    timestamp = train_df.index.to_timestamp().to_list()
    datetime_timestamp = [timestamp.to_pydatetime() for timestamp in timestamp]
    average_reading = train_df["reading_average"].to_list()

    preds_timestamp = preds_df.index.to_timestamp().to_list()
    datetime_preds_timestamp = [timestamp.to_pydatetime() for timestamp in preds_timestamp]
    preds_average_reading = preds_df["reading_average"].to_list()

    results = {
        "timestamp": datetime_timestamp,
        "average_reading": average_reading,
        "datetime_preds_timestamp": datetime_preds_timestamp,
        "preds_average_reading": preds_average_reading,
    }

    return results

@api_router.get(
    "/monitoring/metrics",
    response_model=schemas.MonitoringMetrics,
    status_code=200,
)
async def get_metrics() -> Any:
    """
    Get monitoring metrics.
    """

    # Download the data from GCS.
    metrics = read_blob_from(
        bucket=bucket, blob_name=f"metrics_monitoring.parquet"
    )

    # timestamp = metrics.index.to_timestamp().to_list()
    timestamp = metrics.index.to_list()
    # datetime_timestamp = [timestamp.to_pydatetime() for timestamp in timestamp]
    mape = metrics["MAPE"].to_list()

    return {
        "timestamp": timestamp,
        "mape": mape,
    }

@api_router.get(
    "/monitoring/predictions",
    response_model=schemas.MonitoringValues,
    status_code=200,
)
async def get_predictions() -> Any:
    """
    Get forecasted predictions based on the given area and consumer type.
    """

    # Download the data from GCS.
    y_monitoring = read_blob_from(
        bucket=bucket, blob_name=f"y_monitoring.parquet"
    )
    predictions_monitoring = read_blob_from(
            bucket=bucket, blob_name=f"predictions_monitoring.parquet"
        )

    if len(y_monitoring) == 0 or len(predictions_monitoring) == 0:
        raise HTTPException(
            status_code=404,
            detail=f"No data found",
        )

    # Prepare data to be returned.
    y_monitoring_timestamp = y_monitoring.index.to_timestamp().to_list()
    datetime_y_monitoring_timestamp = [timestamp.to_pydatetime() for timestamp in y_monitoring_timestamp]
    y_monitoring_average_reading = y_monitoring["reading_average"].to_list()

    predictions_monitoring_timestamp = predictions_monitoring.index.to_timestamp().to_list()
    datetime_predictions_monitoring_timestamp = [timestamp.to_pydatetime() for timestamp in predictions_monitoring_timestamp]
    predictions_monitoring_average_reading = predictions_monitoring["reading_average"].to_list()

    results = {
        "y_monitoring_timestamp": datetime_y_monitoring_timestamp,
        "y_monitoring_average_reading": y_monitoring_average_reading,
        "predictions_monitoring_timestamp": datetime_predictions_monitoring_timestamp,
        "predictions_monitoring_average_reading": predictions_monitoring_average_reading,
    }

    return results

