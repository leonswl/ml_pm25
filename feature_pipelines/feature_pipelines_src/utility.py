"""Utility Functions"""
import logging
import os
import json
from pathlib import Path
import pandas as pd

from feature_pipelines_src import settings


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

    data_path = Path(os.path.join(save_dir,file_name))
    if not data_path.exists():
        logging.error(FileNotFoundError(f"Cached JSON from {data_path} does not exist.")) 
        return None

    with open(data_path, "r") as f:
        return json.load(f)
    
def load_csv(file_name: str, save_dir: str = settings.OUTPUT_DIR)->pd.DataFrame:
    """
    Function to load cached csv file

    Args:
        file_name: Name of the JSON file.
        save_dir: Directory of the JSON file.

    Returns: pandas dataframe of cached data.   
    """

    data_path = Path(os.path.join(save_dir,file_name))
    if not data_path.exists():
        logging.error(FileNotFoundError(f"Cached JSON from {data_path} does not exist.")) 
        return None
    
    return pd.read_csv(data_path)

def load_parquet(file_name: str, save_dir: str = settings.OUTPUT_DIR)->pd.DataFrame:
    """
    Function to load cached parquet file

    Args:
        file_name: Name of the JSON file.
        save_dir: Directory of the JSON file.

    Returns: pandas dataframe of cached data.   
    """

    data_path = Path(os.path.join(save_dir,file_name))
    if not data_path.exists():
        logging.error(FileNotFoundError(f"Cached JSON from {data_path} does not exist.")) 
        return None
    
    return pd.read_parquet(data_path)