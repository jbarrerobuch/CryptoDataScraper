from .write_df_to_db import *
from .write_df_to_delta import *
from ..lib_deribit import get_currency_list, get_instruments
from ..Agent import Collector
import polars as pl
import json

__all__ = ["write_instruments_table_deribit"]

def write_instruments_table_deribit(agent:Collector, is_active= True,  output_path:str=None, verbose=False):
    """
    Writes the instruments table to the database.\n
    
    **Parameters:**\n
    agent (object): The the agent collector used to fetch currency list and instruments.\n
    db_conn (object): The database connection object used to write data to the database.\n
    is_active (bool): A flag to indicate whether to update active or expired instruments. Defaults to True.\n
    \n
    **Returns:**\n
    None\n
    """

    if agent.conn == None and output_path == None:
        raise ValueError("Output type is required: db_conn and output_path cannot both be None")
    
    currencies = get_currency_list(deribit_obj=agent.deribit)
    if verbose:
        print(f"Updating instruments with active status {is_active}")

    df_instruments = get_instruments(deribit_obj=agent.deribit, currency_list= currencies, expired= not is_active)
    
    # Add columns
    df_instruments = df_instruments.with_columns(
        [
            pl.lit(False).alias("is_complete"),
            pl.col("tick_size_steps").apply(json.dumps).alias("tick_size_steps"),
            pl.lit("deribit").alias("exchange")
        ]
    )

    if verbose:
        print(df_instruments.head())

    # Write to database
    table_name = "instruments"

    # Review the write functions fro consistency
    write_df_to_db(
        agent=agent,
        data=df_instruments,
        table_name=table_name,
        output_path=output_path,
        exchange="deribit",
        verbose=verbose
    )