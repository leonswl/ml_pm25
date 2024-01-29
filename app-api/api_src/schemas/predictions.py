from typing import Any
import pandas as pd
from pydantic import BaseModel
from datetime import datetime

class PredictionResults(BaseModel):
    timestamp: list[datetime]
    average_reading: list[float]
    datetime_preds_timestamp: list[datetime]
    preds_average_reading: list[float]


class MonitoringMetrics(BaseModel):
    timestamp: list[datetime]
    mape: list[float]


class MonitoringValues(BaseModel):
    y_monitoring_timestamp: list[datetime]
    y_monitoring_average_reading: list[float]
    predictions_monitoring_timestamp: list[datetime]
    predictions_monitoring_average_reading: list[float]