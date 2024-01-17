from pathlib import Path
from typing import Optional
import datetime
from json import JSONDecodeError
import requests
from datetime import datetime, timedelta

import pandas as pd
import numpy as np

from feature_pipelines_src.utility import get_logger, load_json
from feature_pipelines_src import settings

# set up logging
logger = get_logger(__name__)


def extract(
        url:str,
        feature_group_version: int = 1,
        ):
    """
    Extract records using API GET request

    Args:
        url [str]:
        cache_dir [Path]:
        start_date [str]: Optional; start date of data extraction
        end_date [str]: Optional; end date of data extraction
    Returns:
    
    """

    # create cache directory for data and metadata if not exist
   

    # read cached metadata
    metadata = load_json("feature_pipeline_metadata.json")  
    url = "https://api.data.gov.sg/v1/environment/pm25"

    if metadata is not None:
        start_date = metadata['start_date']
        end_date = metadata['end_date']
        cache_dir = metadata['cache_dir']
        empty_records_lst = metadata['empty_records']
        feature_group_version = metadata['feature_group_version']

    # instantiate metadata if does not exist
    elif metadata is None:
        metadata = {}

        cache_dir = settings.OUTPUT_DIR / "data"
        cache_dir.mkdir(parents=True, exist_ok=True)
        empty_records_lst = []

    # compute extraction window
    extraction_window, start_date, end_date = _compute_extraction_window(
        cache_dir=cache_dir,
        metadata=metadata)
    
    # initialise empty list to hold each daily dataframe of parsed json records from the retrieved response
    df_records_lst = []
    for date in extraction_window:

        # define params
        params = {
            "date": date.strftime('%Y-%m-%d')
        }

        # extract and parse data using API
        response_json = _extract_from_api(url, params, date)
    
        # transform data format into required dataframe format
        records = response_json['items']
        if records:
            df_records = _explode_columns(records)
            df_records_lst.append(df_records)
        else:
            empty_records_lst.append(str(date))

    # combine daily records together
    df_records_total = pd.concat(df_records_lst, axis=0).sort_values(by='timestamp').reset_index(drop=True)

    # update metadata with the new extraction window start and end dates
    metadata['start_date'] = str(start_date)
    metadata['end_date']= str(end_date)
    metadata['cache_dir'] = str(cache_dir)
    metadata['empty_records'] = empty_records_lst
    metadata['feature_group_version'] = feature_group_version

    return df_records_total, metadata

def _compute_extraction_window(
        cache_dir:Path,
        metadata:Optional[dict]=None,
        data_origin_date:Optional[str]='2016-02-10'
        )->pd.DatetimeIndex:
        """
        Function to compute extraction window based on optional start and end dates.

        Args:
            start_date [str]: Optional; start date for data extraction. If None is given, defaults to '2016-02-10', the first record of data available validated on 2023-12-15.

            end_date [str]: end date for data extraction. If None is given, defaults to today.

        Returns:
            date_range [pandas DatetimeIndex]: date range in pandas DatetimeIndex format.
        """

        end_date = datetime.today().strftime('%Y-%m-%d')

        # data not extracted, pull all data from the API
        if metadata == {}:
            logger.info("No metadata available.")
            extraction_start_date = data_origin_date
            extraction_end_date = end_date

            date_range = pd.date_range(start=extraction_start_date, end=extraction_end_date)
            return date_range, extraction_start_date, extraction_end_date

        # data is outdated, pull difference between latest cached date and today's date
        else:
            start_date = metadata['start_date']
            extraction_start_date = metadata['end_date']
            extraction_end_date = end_date
            logger.info("Metadata detected.")

            date_range = pd.date_range(start=extraction_start_date,end=extraction_end_date)
            return date_range, start_date, end_date

            # # extraction date falls within cached dates, load cached data since data already exist 
            # if (cached_start_date >= data_origin_date) & (cached_end_date <= today_date):
            #     # extract data from cached data 
            #     file_path = cache_dir / "PM25_Hourly.csv"
            #     if not file_path.exists():
            #         logger.info(f"Downloading data from: {file_path}")

            # # extraction date falls outside of both start and end range. data will be extracted from both ends excluded cached data range
            # elif (today_date < cached_start_date) & (today_date > cached_end_date):
            #     extraction_start_date = start_date
            #     extraction_end_date = end_date
            #     date_range1 = pd.date_range(start_date,datetime.strptime(cached_start_date,'%Y-%m-%d')-timedelta(days=1))
            #     date_range2 = pd.date_range(datetime.strptime(cached_end_date,'%Y-%m-%d')+timedelta(days=1),end_date)
            #     date_range = date_range1.union(date_range2)
            
            # # extraction date 
            # elif (start_date < cached_start_date) & (end_date <= cached_end_date):
            #     extraction_start_date = start_date
            #     extraction_end_date = cached_start_date
            #     date_range = pd.date_range(extraction_start_date,extraction_end_date)

            # elif (start_date >= cached_start_date) & (end_date > cached_end_date):
            #     extraction_start_date = datetime.strptime(cached_end_date,'%Y-%m-%d')+timedelta(days=1)
            #     extraction_end_date = end_date
            #     date_range = pd.date_range(extraction_start_date,extraction_end_date)
           
            

def _extract_from_api(url:str, params:dict, date:str)->dict:
        """
        Function to perform data extraction using API GET request and parse response

        Args:
            url [str]: API url
            params [dict]: dictionary of parameters for GET request

        Returns:
            response_json [dict]: parsed response in json format
        """
        # use API to get data
        try:
            response = requests.get(\
                url=url,
                params=params)
            logger.info(f"Successfully extracted data for {date}")
        except requests.exceptions.HTTPError as e:
            logger.error(
                f" Could not download the file due to: {e}"
            )
        finally:
            logger.info(
                f"Response status = {response.status_code}"
                )
        
        if response.status_code != 200:
            raise ValueError(f"Response status = {response.status_code}. Could not extract records using API on {date}.")

        # parse API response
        try:
            response_json = response.json()
        except JSONDecodeError:
            logger.error(
                        f"Response status = {response.status_code}. Could not decode response from API with URL: {url}"
                    )
            return None
        
        return response_json

def _explode_columns(records:list)->pd.DataFrame:
    """
    Function to normalise json records into a dataframe of features. 
    Readings contain nested dictionary values. This function will explode the values into a dataframe of columns.

    Args:
        records [list]: list of records in json 

    Returns
        df [pandas dataframe]: normalized data in pandas dataframe
    
    """
    # convert records input into dataframe
    df_all_records = pd.DataFrame.from_records(records)

    # explode reading column into a dataframe of columns
    df_readings = pd.json_normalize(df_all_records['readings']) 

    # drop column 'reading'
    df_all_records.drop(['readings'],axis=1,inplace=True)

    # fillna with mode for update timestamp
    df_all_records['update_timestamp'].replace('', np.nan, inplace=True) # replace empty str dates with NaN
    df_all_records['update_timestamp'].fillna(df_all_records['update_timestamp'].mode()[0], inplace=True) # fillna with mode

    # horizontal join records and readings
    df = pd.concat([df_all_records, df_readings],axis=1)

    return df

