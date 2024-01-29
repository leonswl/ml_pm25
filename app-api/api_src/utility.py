
import pandas as pd
from typing import Optional
from google.cloud import storage

from api_src.settings import SETTINGS


def get_bucket(
    bucket_name: str = SETTINGS["APP_API_GCP_BUCKET"],
    bucket_project: str = SETTINGS["APP_API_GCP_PROJECT"],
    json_credentials_path: str = SETTINGS[
        "APP_API_GCP_SERVICE_ACCOUNT_JSON_PATH"],
) -> storage.Bucket:
    """Get a Google Cloud Storage bucket.

    This function returns a Google Cloud Storage bucket that can be used to upload and download
    files from Google Cloud Storage.

    Args:
        bucket_name : str
            The name of the bucket to connect to.
        bucket_project : str
            The name of the project in which the bucket resides.
        json_credentials_path : str
            Path to the JSON credentials file for your Google Cloud Project.

    Returns
        storage.Bucket
            A storage bucket that can be used to upload and download files from Google Cloud Storage.
    """

    storage_client = storage.Client.from_service_account_json(
        json_credentials_path=json_credentials_path,
        project=bucket_project,
    )
    bucket = storage_client.bucket(bucket_name=bucket_name)

    return bucket


def read_blob_from(bucket: storage.Bucket, blob_name: str) -> Optional[pd.DataFrame]:
    """Reads a blob from a bucket and returns a dataframe.

    Args:
        bucket: The bucket to read from.
        blob_name: The name of the blob to read.

    Returns:
        A dataframe containing the data from the blob.
    """

    blob = bucket.blob(blob_name=blob_name)
    if not blob.exists():
        return None

    with blob.open("rb") as f:
        return pd.read_parquet(f)