from typing import Tuple
import hopsworks
import pandas as pd

from settings import SETTINGS

def load_dataset_from_feature_store(
    feature_view_version: int, 
    training_dataset_version: int, 
    fh: int = 24
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load features from feature store.

    Args:
        feature_view_version (int): feature store feature view version to load data from
        training_dataset_version (int): feature store training dataset version to load data from
        fh (int, optional): Forecast horizon. Defaults to 24.

    Returns:
        Train and test splits loaded from the feature store as pandas dataframes.
    """

    project = hopsworks.login(
        api_key_value=SETTINGS["FS_API_KEY"], 
        project=SETTINGS["FS_PROJECT_NAME"]
    )
    fs = project.get_feature_store()

    feature_view = fs.get_feature_view(
            name="pm25_singapore_view", version=feature_view_version
        )
    data, _ = feature_view.get_training_data(
        training_dataset_version=training_dataset_version
    )