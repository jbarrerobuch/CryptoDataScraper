from .write_df_to_db import *
from .write_df_to_parquet import *
from .. import deribit
import psycopg2
import ccxt
import json
import datetime as dt
from ...variables import *

def write_instruments_table_deribit(deribit_obj:ccxt.Exchange, is_active= True, db_conn:psycopg2.connect=None,  output_path:str=None):
    """
    Writes the instruments table to the database.\n
    
    **Parameters:**\n
    deribit_obj (object): The Deribit object used to fetch currency list and instruments.\n
    db_conn (object): The database connection object used to write data to the database.\n
    is_active (bool): A flag to indicate whether to update active or expired instruments. Defaults to True.\n
    \n
    **Returns:**\n
    None\n
    """

    if db_conn is None and output_path is None:
        raise ValueError("Output type is required: db_conn and output_path cannot both be None")

    currencies = deribit.get_currency_list(deribit_obj=deribit_obj)
    #print(f"Updating instruments with active status {is_active}")

    instruments = deribit.get_instruments(deribit_obj=deribit_obj, currency_list= currencies, expired= not is_active)
    instruments["is_complete"] = False
    instruments["tick_size_steps"] = instruments["tick_size_steps"].apply(json.dumps)
    #print(instruments.head())

    # Write to database
    table_name = "instruments"
    if output_path is None:
        write_df_to_db(db_conn=db_conn, data=instruments, table_name=table_name, verbose="False")
    else:
        write_df_to_parquet(
            data=instruments,
            output_path=f"{output_path}/{table_name}/{dt.datetime.now()}",
            storage_options = {
                'anon': False,
                'key': aws_access_key,
                'secret': aws_access_secrete_key,
                'client_kwargs': {'region_name': 'us-east-1'}
            }
        )
    
    #print(write_in_db)