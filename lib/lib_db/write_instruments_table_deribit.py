from .write_df_to_db import *
from .write_df_to_parquet import *
from ..lib_deribit import get_currency_list, get_instruments
from ..Agent import Collector
import psycopg2
import ccxt
import json
import datetime as dt

__all__ = ["write_instruments_table_deribit"]

def write_instruments_table_deribit(agent:Collector, is_active= True,  output_path:str=None):
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
    
    deribit_obj = agent.deribit
    currencies = get_currency_list(deribit_obj=deribit_obj)
    #print(f"Updating instruments with active status {is_active}")

    df_instruments = get_instruments(deribit_obj=deribit_obj, currency_list= currencies, expired= not is_active)
    df_instruments["is_complete"] = False
    df_instruments["tick_size_steps"] = df_instruments["tick_size_steps"].apply(json.dumps)
    #print(instruments.head())

    # Write to database
    table_name = "instruments"

    # Review the write functions fro consistency
    if agent.db_type == "postgres" or agent.db_type == "athena":
        write_df_to_db(
            agent=agent,
            data=df_instruments,
            table_name=table_name,
            output_path=output_path,
            exchange="deribit",
            verbose="False"
        )
    
    else:
        write_df_to_parquet(
            data=df_instruments,
            output_path=f"{output_path}/{table_name}",
        )