from typing import List
import requests

import pandas as pd
import plotly.graph_objects as go

from settings import API_URL


def build_data_plot():
    """
    Build plotly graph for data.
    """

    # Get predictions from API.
    response = requests.get(
        API_URL / "predictions", verify=False
    )
    if response.status_code != 200:
        # If the response is invalid, build empty dataframes in the proper format.
        train_df = build_dataframe([], [])
        preds_df = build_dataframe([], [])

        title = "NO DATA AVAILABLE"
    else:
        json_response = response.json()

        # Build DataFrames for plotting.
        timestamp = json_response.get("timestamp")
        average_reading = json_response.get("average_reading")
        pred_timestamp = json_response.get("datetime_preds_timestamp")
        pred_average_reading = json_response.get("preds_average_reading")

        train_df = build_dataframe(timestamp, average_reading)
        preds_df = build_dataframe(pred_timestamp, pred_average_reading)

        title = "Hourly average reading (PM25)"

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
        y=train_df["average_reading"],
        name="Observations",
        line=dict(color="#C4B6B6"),
        hovertemplate="<br>".join(["Datetime: %{x}", "Average Reading: %{y}"]),
    )
    fig.add_scatter(
        x=preds_df["timestamp"],
        y=preds_df["average_reading"],
        name="Predictions",
        line=dict(color="#FFC703"),
        hovertemplate="<br>".join(["Datetime: %{x}", "Average Reading: %{y}"]),
    )

    return fig


def build_dataframe(timestamp: List[int], average_reading_values: List[float]):
    """
    Build DataFrame for plotting from timestamps and energy consumption values.

    Args:
        timestamp (List[int]): list of timestamp values in UTC
        values (List[float]): list of energy consumption values
    """

    df = pd.DataFrame(
        list(zip(timestamp, average_reading_values)),
        columns=["timestamp", "average_reading"],
    )
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # Resample to hourly frequency to make the data continuous.
    df = df.set_index("timestamp")
    df = df.resample("H").asfreq()
    df = df.reset_index()

    return df