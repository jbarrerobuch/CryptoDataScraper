import pandas as pd
from .. import deribit
from . import read_last_date_from_instruments, write_df_to_db
import datetime as dt
from ..delete_df import delete_df

def write_marketdata(db_conn, deribit_obj, is_active:bool=True, is_complete:bool=False, outputpath:str=None) -> None:
    """
    Gathers instrument data from the DB including the last datapoint's timestamps and gets\n
    market data of each from the Exchange. 
    Then writes all the market data in the DB.
    """
    
    # initialize counters for stats
    total_writen_rows = 0
    total_failed_rows = 0

    # Initialize containers for data storage and control flow
    data_to_write = pd.DataFrame()
    instrument_count = 0

    # Query instruments in data base
    instruments = read_last_date_from_instruments(
        db_conn=db_conn,
        is_active=is_active,
        is_complete=is_complete
        )
    
    # Iterate over the instruments
    for index,row in instruments.iterrows():
        if row["max_timestamp"]:
            #print(row)
            market_data = deribit.fetch_ohlcv(
                deribit_obj=deribit_obj,
                instrument_name=row["instrument_name"],
                instrument_id=row["instrument_id"],
                start_timeframe=row["max_timestamp"] + dt.timedelta(minutes=1),
                end_timeframe=row["expiration_timestamp"]
            )
        else:
            #print(row)
            market_data = deribit.fetch_ohlcv(
                deribit_obj=deribit_obj,
                instrument_name=row["instrument_name"],
                instrument_id=row["instrument_id"],
                start_timeframe=row["creation_timestamp"],
                end_timeframe=row["expiration_timestamp"]
            )
        #if market_data.empty:
        #    print(f"Return empty get market data\n\
        #            {row['instrument_name']} from {row['max_timestamp']}")

        # Collect data for storage
        if not market_data.empty:
            if data_to_write.empty:
                data_to_write = market_data
            else:
                data_to_write = pd.concat([data_to_write, market_data], ignore_index=True)
            instrument_count += 1

        if instrument_count >= 100 and len(data_to_write) > 0:

            data_to_write.reset_index(inplace=True, drop=True)
            status, writen_rows, failed_rows = write_df_to_db(
                db_conn=db_conn,
                data=data_to_write,
                table_name="market_data",
                print_msg=False # f"{data_to_write['instrument_name'].unique()}"
                )
            
            # Add stats to the totals
            total_writen_rows += writen_rows
            total_failed_rows += failed_rows

            # Clear collector df
            
            delete_df(data_to_write)
            #data_to_write = pd.DataFrame()
            instrument_count = 0
            
            #print("Succesful write in db: ", write_in_db)

    # Write remaining data in the df
    if not data_to_write.empty:
        data_to_write.reset_index(inplace=True, drop=True)        
        status, writen_rows, failed_rows = write_df_to_db(
            db_conn=db_conn,
            data=data_to_write,
            table_name="market_data",
            print_msg=False #f"{data_to_write['instrument_name'].unique()}"
            )

        # Add stats to the totals
        total_writen_rows += writen_rows
        total_failed_rows += failed_rows

    # Clear collector df
    delete_df(data_to_write)

    #print("Succesful write in db: ", write_in_db)
    
    return total_writen_rows, total_failed_rows