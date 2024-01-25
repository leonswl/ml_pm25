import gcsfs
from typing import Any, List

import pandas as pd
from fastapi import APIRouter, HTTPException

from api_src import schemas
from api_src.config import get_settings


fs = gcsfs.GCSFileSystem(
    project=get_settings().GCP_PROJECT,
    token=get_settings().GCP_SERVICE_ACCOUNT_JSON_PATH,
)

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
    train_df = pd.read_parquet(f"{get_settings().GCP_BUCKET}/y.parquet", filesystem=fs)
    preds_df = pd.read_parquet(
        f"{get_settings().GCP_BUCKET}/predictions.parquet", filesystem=fs
    )

    if len(train_df) == 0 or len(preds_df) == 0:
        raise HTTPException(
            status_code=404,
            detail=f"No data found"
        )

    # Return only the latest week of observations.
    train_df = train_df.sort_index().tail(24 * 7)

    # Prepare data to be returned.
    timestamp = train_df.index.to_list()
    average_reading = train_df["reading_average"].to_list()

    preds_timestamp = preds_df.index.to_list()
    preds_average_reading = preds_df["reading_average"].to_list()

    results = {
        "timestamp": timestamp,
        "average_reading": average_reading,
        "preds_timestamp": preds_timestamp,
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
    metrics = pd.read_parquet(
        f"{get_settings().GCP_BUCKET}/metrics_monitoring.parquet", filesystem=fs
    )

    timestamp = metrics.index.to_list()
    mape = metrics["MAPE"].to_list()

    return {
        "timestamp": timestamp,
        "mape": mape,
    }

async def get_predictions(area: int, consumer_type: int) -> Any:
    """
    Get forecasted predictions based on the given area and consumer type.
    """

    # Download the data from GCS.
    y_monitoring = pd.read_parquet(
        f"{get_settings().GCP_BUCKET}/y_monitoring.parquet", filesystem=fs
    )
    predictions_monitoring = pd.read_parquet(
        f"{get_settings().GCP_BUCKET}/predictions_monitoring.parquet", filesystem=fs
    )

    if len(y_monitoring) == 0 or len(predictions_monitoring) == 0:
        raise HTTPException(
            status_code=404,
            detail=f"No data found",
        )

    # Prepare data to be returned.
    y_monitoring_timestamp = y_monitoring.index.to_list()
    y_monitoring_average_reading = y_monitoring["energy_consumption"].to_list()

    predictions_monitoring_timestamp = predictions_monitoring.index.to_list()
    predictions_monitoring_average_reading = predictions_monitoring["energy_consumption"].to_list()

    results = {
        "y_monitoring_timestamp": y_monitoring_timestamp,
        "y_monitoring_average_reading": y_monitoring_average_reading,
        "predictions_monitoring_timestamp": predictions_monitoring_timestamp,
        "predictions_monitoring_average_reading": predictions_monitoring_average_reading,
    }

    return results

