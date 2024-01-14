import logging
import json
import joblib
import os
import pandas as pd
import wandb

from pathlib import Path
from typing import Union, Optional

from training_pipelines_src import settings

def get_logger(name: str) -> logging.Logger:
    """
    Template for getting a logger.

    Args:
        name: Name of the logger.

    Returns: Logger.
    """

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(name)

    return logger

def save_json(data: dict, file_name: str, save_dir: str = settings.OUTPUT_DIR):
    """
    Save a dictionary as a JSON file.

    Args:
        data: data to save.
        file_name: Name of the JSON file.
        save_dir: Directory to save the JSON file.

    Returns: None
    """
    data_path = os.path.join(save_dir,file_name)
    with open(data_path, "w") as f:
        json.dump(data, f)

def load_json(file_name: str, save_dir: str = settings.OUTPUT_DIR) -> dict:
    """
    Load a JSON file.

    Args:
        file_name: Name of the JSON file.
        save_dir: Directory of the JSON file.

    Returns: Dictionary with the data.
    """

    data_path = Path(save_dir) / file_name
    if not data_path.exists():
        logging.error(FileNotFoundError(f"Cached JSON from {data_path} does not exist.")) 
        return None

    with open(data_path, "r") as f:
        return json.load(f)
    
def save_model(model, model_path: Union[str, Path]):
    """
    Template for saving a model.

    Args:
        model: Trained model.
        model_path: Path to save the model.
    """

    joblib.dump(model, model_path)


def load_model(model_path: Union[str, Path]):
    """
    Template for loading a model.

    Args:
        model_path: Path to the model.

    Returns: Loaded model.
    """

    return joblib.load(model_path)


def init_wandb_run(
    name: str,
    group: Optional[str] = None,
    job_type: Optional[str] = None,
    add_timestamp_to_name: bool = False,
    run_id: Optional[str] = None,
    resume: Optional[str] = None,
    reinit: bool = False,
    project: str = settings.SETTINGS["WANDB_PROJECT"],
    entity: str = settings.SETTINGS["WANDB_ENTITY"],
):
    """Wrapper over the wandb.init function."""

    if add_timestamp_to_name:
        name = f"{name}_{pd.Timestamp.now().strftime('%Y-%m-%d_%H-%M-%S')}"

    run = wandb.init(
        project=project,
        entity=entity,
        name=name,
        group=group,
        job_type=job_type,
        id=run_id,
        reinit=reinit,
        resume=resume,
        settings=wandb.Settings(start_method="fork")
    )

    return run
