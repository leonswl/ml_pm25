from typing import List
import requests

import pandas as pd
import plotly.graph_objects as go

from settings import API_URL


def build_metrics_plot():
    """
    Build plotly graph for metrics.
    """

    response = requests.get(API_URL / "monitoring" / "metrics", verify=False)
    if response.status_code != 200:
        # If the response is invalid, build empty dataframes in the proper format.
        metrics_df = build_dataframe([], [], values_column_name="mape")

        title = "No metrics available."
    else:
        json_response = response.json()

        # Build DataFrame for plotting.
        timestamp = json_response.get("timestamp", [])
        mape = json_response.get("mape", [])
        metrics_df = build_dataframe(timestamp, mape, values_column_name="mape")

        title = "Predictions vs. Observations | MAPE"

    # Create plot.
    fig = go.Figure()
    fig.update_layout(
        title=dict(
            text=title,
            font=dict(family="Arial", size=16),
        ),
        showlegend=True,
    )
    fig.update_xaxes(title_text="Datetime UTC")
    fig.update_yaxes(title_text="MAPE")
    fig.add_scatter(
        x=metrics_df["timestamp"],
        y=metrics_df["mape"],
        name="MAPE",
        line=dict(color="#C4B6B6"),
        hovertemplate="<br>".join(["Datetime UTC: %{x}", "MAPE: %{y} kWh"]),
    )

    return fig


def build_data_plot():
    """
    Build plotly graph for data.
    """

    # Get predictions from API.
    response = requests.get(
        API_URL / "monitoring" / "predictions", verify=False
    )
    if response.status_code != 200:
        # If the response is invalid, build empty dataframes in the proper format.
        train_df = build_dataframe([], [])
        preds_df = build_dataframe([], [])

        title = "NO DATA AVAILABLE"
    else:
        json_response = response.json()

        # Build DataFrames for plotting.
        y_monitoring_timestamp = json_response.get("y_monitoring_timestamp")
        y_monitoring_average_reading = json_response.get("y_monitoring_average_reading")
        predictions_monitoring_timestamp = json_response.get("predictions_monitoring_timestamp")
        predictions_monitoring_average_reading = json_response.get("predictions_monitoring_average_reading")

        train_df = build_dataframe(y_monitoring_timestamp, y_monitoring_average_reading)
        preds_df = build_dataframe(predictions_monitoring_timestamp, predictions_monitoring_average_reading)

        title = "Predictions vs. Observations | Hourly Average reading (PM25)"

    # Create plot.
    fig = go.Figure()
    fig.update_layout(
        title=dict(
            text=title,
            font=dict(family="Arial", size=16),
        ),
        showlegend=True,
    )
    fig.update_xaxes(title_text="timestamp")
    fig.update_yaxes(title_text="Average Reading")
    fig.add_scatter(
        x=train_df["timestamp"],
        y=train_df["reading_average"],
        name="Observations",
        line=dict(color="#C4B6B6"),
        hovertemplate="<br>".join(["Datetime: %{x}", "Average Reading: %{y} kWh"]),
    )
    fig.add_scatter(
        x=preds_df["timestamp"],
        y=preds_df["reading_average"],
        name="Predictions",
        line=dict(color="#FFC703"),
        hovertemplate="<br>".join(["Datetime: %{x}", "Average Reading: %{y} kWh"]),
    )

    return fig


def build_dataframe(
        timestamp: List[int], 
        average_reading_values: List[float],
        values_column_name: str = "reading_average"):
    """
    Build DataFrame for plotting from timestamps and energy consumption values.

    Args:
        timestamp (List[int]): list of timestamp values in UTC
        values (List[float]): list of energy consumption values
        values_column_name (str): name of the column containing the values
    """

    df = pd.DataFrame(
        list(zip(timestamp, average_reading_values)),
        columns=["timestamp", values_column_name],
    )
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # Resample to hourly frequency to make the data continuous.
    df = df.set_index("timestamp")
    df = df.resample("H").asfreq()
    df = df.reset_index()

    return df