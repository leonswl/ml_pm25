import pandas as pd

from feature_pipelines_src.utility import load_parquet

def transform(data: pd.DataFrame, cache_dir:str) -> pd.DataFrame:
    """
    Wrapper function to apply data transformation on extracted data

    Args:
        df [pandas dataframe]: extracted records in pandas dataframe

    Returns:
        df [pandas dataframe]: transformed records in pandas dataframe
    
    """
    data = _rename_columns(data)
    data = _compute_average_psi(data)
    data = _cast_columns(data)
    data = _update_data(data, cache_dir)

    return data

def _update_data(df:pd.DataFrame, cache_dir:str):
    """
    Function to update dataset with newly extracted data. 

    Args:
        df [pandas dataframe]: new data that is extracted based on latest run
        cache_dir [str]: path directory of cached output

    Returns:
        completed_df [pandas dataframe]: updated data 
    
    """

    # merge cached data and newly extracted data together

    cached_df = load_parquet(
        file_name="PM25_Hourly.parquet",
        save_dir=cache_dir)
    
    if cached_df is None:
        return df

    else:
        # drop overlapping hourly data
        # find earliest timestamp in new data and check if timestamp exist in cached data
        # drop overlapped existing data in cached data 
        cached_df.drop(cached_df.loc[cached_df['timestamp'] >= df['timestamp'].min()].index, inplace=True) 
        
        # merge cached and newly extracted data
        completed_df = pd.concat([cached_df, df],axis=0).sort_values(by='timestamp').reset_index(drop=True)

        # ensure timestamp is in pandas datetime format
        columns = ['timestamp','update_timestamp']
        for col in columns:
            # df[col] = df[col].apply(lambda x: 'T'.join(x.split(" ")))
            completed_df[col] = pd.to_datetime(completed_df[col], utc=True)

        return completed_df

def _rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename columns to match our schema.
    """

    data = df.copy()

    # drop irrelevant columns

    # Rename columns
    data.rename(
        columns={
            "pm25_one_hourly.west": "reading_west",
            "pm25_one_hourly.east": "reading_east",
            "pm25_one_hourly.central": "reading_central",
            "pm25_one_hourly.south": "reading_south",
            "pm25_one_hourly.north": "reading_north",
        },
        inplace=True,
    )

    return data

def _compute_average_psi(df: pd.DataFrame)->pd.DataFrame:
    """
    Compute new column with average PSI readings for all regions
    """

    data = df.copy()
    reading_columns_lst = ['reading_west','reading_east','reading_central','reading_central','reading_south','reading_north']
    data['reading_average'] = (data[reading_columns_lst].sum(axis=1))/5

    return data


def _cast_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cast columns to the correct data type.
    """

    data = df.copy()

    data["timestamp"] = pd.to_datetime(data["timestamp"],format="mixed", utc=True)
    data["update_timestamp"] = pd.to_datetime(data["update_timestamp"])
    data["reading_west"] = data["reading_west"].astype("int32")
    data["reading_east"] = data["reading_east"].astype("int32")
    data["reading_central"] = data["reading_central"].astype("int32")
    data["reading_south"] = data["reading_south"].astype("int32")
    data["reading_north"] = data["reading_north"].astype("int32")
    data["reading_average"] = data["reading_average"].astype("int32")

    return data



