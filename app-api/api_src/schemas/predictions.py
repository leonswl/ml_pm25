from typing import Any

from pydantic import BaseModel


class PredictionResults(BaseModel):
    timestamp: list[int]
    preds_average_reading: list[float]


class MonitoringMetrics(BaseModel):
    timestamp: list[int]
    mape: list[float]


class MonitoringValues(BaseModel):
    y_monitoring_datetime_utc: list[int]
    y_monitoring_energy_consumption: list[float]
    predictions_monitoring_datetime_utc: list[int]
    predictions_monitoring_energy_consumptionc: list[float]