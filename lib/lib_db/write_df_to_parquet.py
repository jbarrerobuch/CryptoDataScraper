import pandas as pd
import polars as pl
from delta import DeltaTable

__all__ = ["write_df_to_parquet"]

def write_df_to_parquet(data: pd.DataFrame, output_path: str, partition_cols: list = [], storage_options: dict = {}) -> None:
    """
    Write a dataframe to a parquet file, either locally or to an S3 bucket.\n

    **Args:**\n
        data (pd.DataFrame): The dataframe to write.\n
        output_path (str): The path to write the parquet file to. Can be a local path or an S3 bucket path (e.g. 's3://my_bucket/my_data.parquet').

    **Returns:**\n
        None\n
    """
    data.to_parquet(
        path=output_path,
        engine="fastparquet",
        compression="snappy",
        index=False,
        partition_cols=partition_cols,
        storage_options=storage_options
        )
    
    return (True, len(data), 0)