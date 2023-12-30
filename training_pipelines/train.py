import json
from collections import OrderedDict
import os
from pathlib import Path
from typing import OrderedDict as OrderedDictType, Optional, Tuple

import fire
import hopsworks
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import wandb
from sktime.performance_metrics.forecasting import (
    mean_squared_percentage_error,
    mean_absolute_percentage_error,
)
from sktime.utils.plotting import plot_series

import utility
from settings import SETTINGS, OUTPUT_DIR
from data import load_dataset_from_feature_store
from models import build_model, build_baseline_model

logger = utility.get_logger(__name__)

def from_best_config(
    fh: int = 24,
    feature_view_version: Optional[int] = None,
    training_dataset_version: Optional[int] = None,
    X: Optional[bool] = False
) -> dict:
    """Train and evaluate on the test set the best model found in the hyperparameter optimization run.
    After training and evaluating it uploads the artifacts to wandb & hopsworks model registries.

    Args:
        fh (int, optional): Forecasting horizon. Defaults to 24.
        feature_view_version (Optional[int], optional): feature store - feature view version.
             If none, it will try to load the version from the cached feature_view_metadata.json file. Defaults to None.
        training_dataset_version (Optional[int], optional): feature store - feature view - training dataset version.
            If none, it will try to load the version from the cached feature_view_metadata.json file. Defaults to None.
        X [bool]: Presence of exoggnous variables. Defaults to false (without exogenous variables)

    Returns:
        dict: Dictionary containing metadata about the training experiment.
    """
    feature_view_metadata = utility.load_json("feature_view_metadata.json")
    if feature_view_version is None:
        feature_view_version = feature_view_metadata["feature_view_version"]
    if training_dataset_version is None:
        training_dataset_version = feature_view_metadata["training_dataset_version"]

    if X is False:
        y_train, y_test = load_dataset_from_feature_store(
            feature_view_version=feature_view_version,
            training_dataset_version=training_dataset_version,
            fh=fh,
        )
    elif X is True:
        y_train, y_test, X_train, X_test = load_dataset_from_feature_store(
            feature_view_version=feature_view_version,
            training_dataset_version=training_dataset_version,
            fh=fh,
        )

    training_start_datetime = y_train.index.get_level_values("timestamp").min()
    training_end_datetime = y_train.index.get_level_values("timestamp").max()
    testing_start_datetime = y_test.index.get_level_values("timestamp").min()
    testing_end_datetime = y_test.index.get_level_values("timestamp").max()
    logger.info(
        f"Training model on data from {training_start_datetime} to {training_end_datetime}."
    )
    logger.info(
        f"Testing model on data from {testing_start_datetime} to {testing_end_datetime}."
    )

    with utility.init_wandb_run(
        name="best_model",
        job_type="train_best_model",
        group="train",
        reinit=True,
        add_timestamp_to_name=True,
    ) as run:
        run.use_artifact("split_train:latest")
        run.use_artifact("split_test:latest")
        # Load the best config from sweep.
        best_config_artifact = run.use_artifact(
            "best_config:latest",
            type="model",
        )
        download_dir = best_config_artifact.download()
        config_path = Path(download_dir) / "best_config.json"
        with open(config_path) as f:
            config = json.load(f)
        # Log the config to the experiment.
        run.config.update(config)

        # # Baseline model
        baseline_forecaster = build_baseline_model(seasonal_periodicity=fh)
        if X is False:
            baseline_forecaster = train_model(baseline_forecaster, y_train, fh=fh)
            _, metrics_baseline = evaluate(baseline_forecaster, y_test)
        elif X is True:
            baseline_forecaster = train_model(baseline_forecaster, y_train, X_train, fh=fh)
            _, metrics_baseline = evaluate(baseline_forecaster, y_test, X_test)
        
        # slices = metrics_baseline.pop("slices")
        for k, v in metrics_baseline.items():
            logger.info(f"Baseline test {k}: {v}")
        wandb.log({"test": {"baseline": metrics_baseline}})
        # wandb.log({"test.baseline.slices": wandb.Table(dataframe=slices)})

        # Build & train best model.
        best_model = build_model(config)

        if X is False:
            best_forecaster = train_model(best_model, y_train, fh=fh)
            y_pred, metrics = evaluate(best_forecaster, y_test) # Evaluate best model

        elif X is True:
            best_forecaster = train_model(best_model, y_train, X_train, fh=fh)
            y_pred, metrics = evaluate(best_forecaster, y_test, X_test) # Evaluate best model

        # slices = metrics.pop("slices")
        for k, v in metrics.items():
            logger.info(f"Model test {k}: {v}")
        wandb.log({"test": {"model": metrics}})
        # wandb.log({"test.model.slices": wandb.Table(dataframe=slices)})

        # Render best model on the test set.
        results = OrderedDict({"y_train": y_train, "y_test": y_test, "y_pred": y_pred})
        render(results, output_dir=OUTPUT_DIR, prefix="images_test")

        # Update best model with the test set.
        # NOTE: Method update() is not supported by LightGBM + Sktime. Instead we will retrain the model on the entire dataset.
        if X is False:
            best_forecaster = train_model(
                model=best_forecaster,
                y_train=pd.concat([y_train, y_test]).sort_index(),
                fh=fh,
            )
            y_forecast = forecast(best_forecaster)
        elif X is True:
            best_forecaster = train_model(
                model=best_forecaster,
                y_train=pd.concat([y_train, y_test]).sort_index(),
                X_train=pd.concat([X_train, X_test]).sort_index(),
                fh=fh,
            )
            X_forecast = compute_forecast_exogenous_variables(X_test, fh)
            y_forecast = forecast(best_forecaster, X_forecast)\
            
        logger.info(
            f"Forecasted future values for render in between {y_test.index.min()} and {y_test.index.max()}."
        )
        # logger.info(
        #     f"Forecasted future values for render in between {y_test.index.get_level_values('timestamp').min()} and {y_test.index.get_level_values('timestamp').max()}."
        # )
        results = OrderedDict(
            {
                "y_train": y_train,
                "y_test": y_test,
                "y_forecast": y_forecast,
            }
        )
        # Render best model future forecasts.
        render(results, output_dir=OUTPUT_DIR, prefix="images_forecast")

        # Save best model.
        save_model_path = OUTPUT_DIR / "best_model.pkl"
        utility.save_model(best_forecaster, save_model_path)
        metadata = {
            "experiment": {
                "fh": fh,
                "feature_view_version": feature_view_version,
                "training_dataset_version": training_dataset_version,
                "training_start_datetime": training_start_datetime.to_timestamp().isoformat(),
                "training_end_datetime": training_end_datetime.to_timestamp().isoformat(),
                "testing_start_datetime": testing_start_datetime.to_timestamp().isoformat(),
                "testing_end_datetime": testing_end_datetime.to_timestamp().isoformat(),
            },
            "results": {"test": metrics},
        }
        artifact = wandb.Artifact(name="best_model", type="model", metadata=metadata)
        artifact.add_file(str(save_model_path))
        run.log_artifact(artifact)

        run.finish()
        artifact.wait()

    model_version = add_best_model_to_model_registry(artifact)

    metadata = {"model_version": model_version}
    utility.save_json(metadata, file_name="train_metadata.json")

    return metadata


def train_model(
        model, 
        y_train: pd.DataFrame, 
        fh: int,
        X_train: Optional[pd.DataFrame]=None):
    """Train the forecaster on the given training set and forecast horizon."""

    fh = np.arange(fh) + 1
    if X_train is None:
        model.fit(y_train, fh=fh)
    else:
        model.fit(y_train, X_train,fh=fh)

    return model


def evaluate(
    forecaster, 
    y_test: pd.DataFrame, 
    X_test: Optional[pd.DataFrame]=None
) -> Tuple[pd.DataFrame, dict]:
    """Evaluate the forecaster on the test set by computing the following metrics:
        - RMSPE
        - MAPE
        - Slices: RMSPE, MAPE

    Args:
        forecaster: model following the sklearn API
        y_test (pd.DataFrame): time series to forecast
        X_test (pd.DataFrame): exogenous variables

    Returns:
        The predictions as a pd.DataFrame and a dict of metrics.
    """

    if X_test is None:
        y_pred = forecaster.predict()
    else:
        y_pred = forecaster.predict(X=X_test)

    # fill NaN with meanm temporary fix for bug but this is not the correct fix
    y_pred.fillna(round(y_pred.mean(),0),inplace=True)

    # Compute aggregated metrics.
    results = dict()
    rmspe = mean_squared_percentage_error(y_test, y_pred, squared=False)
    results["RMSPE"] = rmspe
    mape = mean_absolute_percentage_error(y_test, y_pred, symmetric=False)
    results["MAPE"] = mape

    # Compute metrics per slice.
    # y_test_slices = y_test.groupby(["area", "consumer_type"])
    # y_pred_slices = y_pred.groupby(["area", "consumer_type"])
    # slices = pd.DataFrame(columns=["area", "consumer_type", "RMSPE", "MAPE"])
    # for y_test_slice, y_pred_slice in zip(y_test_slices, y_pred_slices):
    #     (area_y_test, consumer_type_y_test), y_test_slice_data = y_test_slice
    #     (area_y_pred, consumer_type_y_pred), y_pred_slice_data = y_pred_slice

    #     assert (
    #         area_y_test == area_y_pred and consumer_type_y_test == consumer_type_y_pred
    #     ), "Slices are not aligned."

    #     rmspe_slice = mean_squared_percentage_error(
    #         y_test_slice_data, y_pred_slice_data, squared=False
    #     )
    #     mape_slice = mean_absolute_percentage_error(
    #         y_test_slice_data, y_pred_slice_data, symmetric=False
    #     )

    #     slice_results = pd.DataFrame(
    #         {
    #             "area": [area_y_test],
    #             "consumer_type": [consumer_type_y_test],
    #             "RMSPE": [rmspe_slice],
    #             "MAPE": [mape_slice],
    #         }
    #     )
    #     slices = pd.concat([slices, slice_results], ignore_index=True)

    # results["slices"] = slices

    return y_pred, results

def render(
    timeseries: OrderedDictType[str, pd.DataFrame],
    output_dir: str,
    prefix: Optional[str] = None,
    delete_from_disk: bool = False,
):
    """Render the timeseries as a single plot per (area, consumer_type) and saves them to disk and to wandb."""

    # grouped_timeseries = OrderedDict()
    fig, ax = plot_series(*timeseries.values(),labels=timeseries.keys())
    fig.suptitle(f"{prefix}")
    # save matplotlib image
    image_save_path = output_dir / f"{prefix}.png"
    plt.savefig(image_save_path)
    plt.close(fig)
    print(f"figure have been saved to {image_save_path}")

    if prefix:
        wandb.log({prefix: wandb.Image(str(image_save_path))})
    else:
        wandb.log(wandb.Image(str(image_save_path)))

    if delete_from_disk:
        os.remove(image_save_path)
    # df = df.reset_index(level=0)
    # groups = df.groupby(["area", "consumer_type"])
    # for group_name, split_group_values in groups:
    #     group_values = grouped_timeseries.get(group_name, {})

    #     grouped_timeseries[group_name] = {
    #         f"{split}": split_group_values["energy_consumption"],
    #         **group_values,
    #     }

    # output_dir = OUTPUT_DIR / prefix if prefix else OUTPUT_DIR
    # output_dir.mkdir(parents=True, exist_ok=True)
    # for group_name, group_values_dict in grouped_timeseries.items():
    #     fig, ax = plot_series(
    #         *group_values_dict.values(), labels=group_values_dict.keys()
    #     )
    #     fig.suptitle(f"Area: {group_name[0]} - Consumer type: {group_name[1]}")

    #     # save matplotlib image
    #     image_save_path = str(output_dir / f"{group_name[0]}_{group_name[1]}.png")
    #     plt.savefig(image_save_path)
    #     plt.close(fig)

    #     if prefix:
    #         wandb.log({prefix: wandb.Image(image_save_path)})
    #     else:
    #         wandb.log(wandb.Image(image_save_path))

    #     if delete_from_disk:
    #         os.remove(image_save_path)


def compute_forecast_exogenous_variables(X_test: pd.DataFrame, fh: int):
    """Computes the exogenous variables for the forecast horizon."""

    X_forecast = X_test.copy()
    X_forecast.index.set_levels(
        X_forecast.index.levels[-1] + fh, level=-1, inplace=True
    )

    return X_forecast


def forecast(forecaster, X_forecast: Optional[pd.DataFrame]=None):
    """
    Function to forecast the energy consumption for the given exogenous variables (optional) and time horizon.
    
    forecaster 
    """
    if X_forecast is None:
        return forecaster.predict()
    
    else:
        return forecaster.predict(X=X_forecast)


def add_best_model_to_model_registry(best_model_artifact: wandb.Artifact) -> int:
    """Adds the best model artifact to the model registry."""

    project = hopsworks.login(
        api_key_value=SETTINGS["FS_API_KEY"], project=SETTINGS["FS_PROJECT_NAME"]
    )

    # Upload the model to the Hopsworks model registry.
    best_model_dir = best_model_artifact.download()
    best_model_path = Path(best_model_dir) / "best_model.pkl"
    best_model_metrics = best_model_artifact.metadata["results"]["test"]

    mr = project.get_model_registry()
    py_model = mr.python.create_model("best_model", metrics=best_model_metrics)
    py_model.save(str(best_model_path))

    return py_model.version


if __name__ == "__main__":
    fire.Fire(from_best_config)