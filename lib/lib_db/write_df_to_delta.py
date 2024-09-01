import pandas as pd
import polars as pl
from deltalake import DeltaTable
__all__ = ["write_df_to_delta"]

def write_df_to_delta(data: pl.DataFrame, output_path: str, table_name:str = None, partition_cols: list = [], storage_options: dict = {}) -> None:
    """
    Write Polars to a delta table in parquet file, either locally or to an S3 bucket.\n

    **Args:**\n
        data (pd.DataFrame): The dataframe to write.\n
        output_path (str): The path to write the parquet file to. Can be a local path or an S3 bucket path (e.g. 's3://my_bucket/my_data.parquet').

    **Returns:**\n
        None\n
    """

    data.write_delta(
        f"{output_path}/{table_name}",
        mode="append",
        delta_write_options={"partition_by": partition_cols, "name": table_name},
        storage_options=storage_options
    )
    
    return (True, len(data), 0)