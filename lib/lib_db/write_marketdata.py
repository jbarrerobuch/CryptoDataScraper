import pandas as pd
import lib.lib_deribit as lib_deribit
from .write_df_to_db import *
from .read_last_date_from_instrument import *
from .write_df_to_parquet import *
import datetime as dt
from ..delete_df import delete_df
from ..Agent import Collector

__all__ = ["write_marketdata"]

def write_marketdata(agent:Collector, is_active:bool=True, is_complete:bool=False, output_path:str=None, exchange:str="deribit") -> None:
    """
	Gathers instrument data from the DB including the last datapoint's timestamps, 
	retrieves market data of each from the Exchange, and writes all the market data in the DB.

	Parameters:
		agent (Collector): The Collector object containing the DB connection and Exchange API credentials.
		is_active (bool): A flag indicating whether to consider only active instruments. Defaults to True.
		is_complete (bool): A flag indicating whether to consider only complete instruments. Defaults to False.
		outputpath (str): The path to store the output data. Defaults to None.

	Returns:
		None
	"""
    
    # initialize counters for stats
    total_writen_rows = 0
    total_failed_rows = 0

    # Initialize containers for data storage and control flow
    data_to_write = pd.DataFrame()
    instrument_count = 0

    # Query instruments in data base
    df_instruments = read_last_date_from_instruments(
        agent=agent,
        is_active=is_active,
        is_complete=is_complete
        )
    
    # Iterate over the instruments
    for index,row in df_instruments.iterrows():
        if row["max_timestamp"]:
            #print(row)
            market_data = lib_deribit.fetch_candles(
                agent=agent,
                instrument_name=row["instrument_name"],
                instrument_id=row["instrument_id"],
                start_timeframe=row["max_timestamp"] + dt.timedelta(minutes=1),
                end_timeframe=row["expiration_timestamp"]
            )
        else:
            #print(row)
            market_data = lib_deribit.fetch_candles(
                agent=agent,
                instrument_name=row["instrument_name"],
                instrument_id=row["instrument_id"],
                start_timeframe=row["creation_timestamp"],
                end_timeframe=row["expiration_timestamp"]
            )

        # Collect data for storage
        if not market_data.empty:
            if data_to_write.empty:
                data_to_write = market_data
            else:
                data_to_write = pd.concat([data_to_write, market_data], ignore_index=True)
                instrument_count += 1

        if instrument_count >= 100 and len(data_to_write) > 0:

            if agent.db_type == "postgres" or agent.db_type == "athena":

                data_to_write.reset_index(inplace=True, drop=True)
                
                # Create partition columns
                data_to_write["exchange"] = exchange
                data_to_write['price_index'] = data_to_write['instrument_name'].str[:3].str.lower() + '_usd'
                data_to_write["instrument_type"] = "option"

                status, writen_rows, failed_rows = write_df_to_db(
                    agent=agent,
                    data=data_to_write,
                    table_name="market_data",
                    output_path=output_path,
                    verbose=False
                )

            elif agent.db_type == None and output_path != None:
                write_df_to_parquet(
                    data=data_to_write,
                    output_path=output_path
                )
            
            # Add stats to the totals
            agent.data_written += writen_rows
            agent.data_failed += failed_rows

            # Clear collector df
            delete_df(data_to_write)
            
            #data_to_write = pd.DataFrame()
            instrument_count = 0
            

    # Write remaining data in the df
    if not data_to_write.empty:
        if agent.db_type == "postgres" or agent.db_type == "athena":

            data_to_write.reset_index(inplace=True, drop=True)
            # Create partition columns
            data_to_write["exchange"] = exchange
            data_to_write['price_index'] = data_to_write['instrument_name'].str[:3].str.lower() + '_usd'
            data_to_write["instrument_type"] = "option"

            status, writen_rows, failed_rows = write_df_to_db(
                agent=agent,
                data=data_to_write,
                table_name="market_data",
                output_path=output_path,
                verbose=False
                )
        elif agent.db_type == None:
            write_df_to_parquet(
                data=data_to_write,
                output_path=output_path
            )
        
        # Add stats to the totals
        agent.data_written += writen_rows
        agent.data_failed += failed_rows

        # Clear collector df
        delete_df(data_to_write)
