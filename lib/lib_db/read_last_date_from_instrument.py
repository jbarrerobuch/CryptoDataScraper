import pandas as pd
import polars as pl
from deltalake import DeltaTable
from .execute_sql import execute_sql
from ..Agent import Collector

__all__ = ["read_last_date_from_instruments"]

def read_last_date_from_instruments(agent:Collector, is_active:bool=True, is_complete:bool=False) -> pd.DataFrame:
    """
    Fetch timestamp columns from the table "instruments" filtered by:
    - is_active (bool): default True
    - is_complete (bool): default False
    - get_last_data_timestamp (bool): defaul False; get last market data timestamp

    and the first (if get_min_date = True) and last timestamp available in market_data

    SQL SNIPPET
    ------------------------------
    SELECT instruments.instrument_name, instruments.creation_timestamp as creation_timestamp, min(timestamp) as min_timestamp,
        instruments.expiration_timestamp as expiration_timestamp, max(timestamp) as max_timestamp
    FROM market_data
    INNER JOIN instruments
    ON market_data.instrument_id = instruments.instrument_id
    GROUP BY instruments.instrument_name, instruments.creation_timestamp, instruments.expiration_timestamp
    """
    if agent.db_type == "postgres":
        # Build SQL snippets for execution
        sql = f"SELECT instruments.instrument_name, instruments.instrument_id, instruments.is_active,\
            instruments.creation_timestamp as creation_timestamp, instruments.expiration_timestamp as expiration_timestamp,\
            max(timestamp) as max_timestamp\
            FROM market_data\
            INNER JOIN instruments ON market_data.instrument_id = instruments.instrument_id\
            WHERE instruments.is_active = {is_active} AND is_complete = {is_complete}\
            GROUP BY instruments.instrument_name, instruments.instrument_id, instruments.is_active, instruments.creation_timestamp, instruments.expiration_timestamp\
            ORDER BY instruments.expiration_timestamp DESC"
            
        # Define dfresult and dtypes
        dtypes = {
            "instrument_name": "str",
            "instrument_id": "str",
            "is_active": "bool",
            "creation_timestamp": "float",
            "expiration_timestamp": "float",
            "max_timestamp": "float"
        }

        dfresult = execute_sql(agent=agent, sql=sql)

        # Convert dtypes to datetime
        dfresult["creation_timestamp"] = pd.to_datetime(dfresult["creation_timestamp"], unit="ms")
        dfresult["expiration_timestamp"] = pd.to_datetime(dfresult["expiration_timestamp"], unit="ms")
        dfresult["max_timestamp"] = pd.to_datetime(dfresult["max_timestamp"], unit="ms")
    
    elif agent.db_type == "delta":
        
        # Read the delta table
        delta_table_path = "data"
        delta_table = DeltaTable(delta_table_path)

        # Convert to Polar DataFrame
        df = pl.DataFrame(delta_table.to_pyarrow_table())

        # Filter the data
        df = df.filter(
            (pl.col("is_active") == is_active) & (pl.col("is_complete") == is_complete)
        )

        # Group by and aggregate
        df = df.groupby(["instrument_name", "instrument_id", "is_active", "creation_timestamp", "expiration_timestamp"]).agg(
            [
                pl.col("timestamp").max().alias("max_timestamp")
            ]
        )

        df = df.with_columns(
            [
                pl.col("creation_timestamp").cast(pl.Datetime("ms")).alias("creation_timestamp"),
                pl.col("expiration_timestamp").cast(pl.Datetime("ms")).alias("expiration_timestamp"),
                pl.col("max_timestamp").cast(pl.Datetime("ms")).alias("max_timestamp")
            ]
        )
    
    return dfresult