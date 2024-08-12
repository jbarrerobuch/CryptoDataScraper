import datetime as dt
import pandas as pd
from .check_index_data_year_complete import check_index_data_year_complete
from .write_df_to_db import write_df_to_db
from ..binance import *
from ..delete_df import delete_df

def write_index_data(db_conn, binance_obj, index_name:str, interval:str, start_timestamp:dt.datetime, end_timestamp:dt.datetime=dt.datetime.now(dt.timezone.utc), exchange:str="binance"):
    """
    Retrive and write index data from Binance.
    Accepted intervals: ['1s', '1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1M']
    """

    #print(f"Start collecting {index_name} since {start_timestamp} to {end_timestamp}")
    #print()
    last_timestamp = check_index_data_year_complete(
        db_conn=db_conn,
        index_name=index_name,
        year=start_timestamp.year,
        exchange="binance"
        )
    
    #print(f"Last timestamp in DB is {last_timestamp}")


    # Check valid intervals
    valid_intervals = ['1s', '1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1M']
    if not interval in valid_intervals:
        print("Invalid interval")
        return

    # Initialize data collector for bulk writing in DB
    data_to_write = pd.DataFrame()

    # Refresh end timestamp to now
    end_timestamp = dt.datetime.now(dt.timezone.utc)

    # Loop until end timestamp is reached
    while last_timestamp < end_timestamp:

        # retrive data from the last available data in the DB or from the startTimestamp defined
        index_data = get_index_data(
            binance_obj=binance_obj,
            index_name=index_name,
            interval=interval,
            start_timestamp=last_timestamp
            )

        if index_data.empty:
            print(f"Returned empty getting data from:\n\ {index_name} - {last_timestamp}")
            last_timestamp = index_data["timestamp"].max().replace(tzinfo=dt.timezone.utc)
        else:
            # Append retrived data to bulk writer
            if data_to_write.empty:
                data_to_write = index_data
            else:
                data_to_write = pd.concat([data_to_write, index_data], ignore_index=True)

            # Write to the DB if the bulk container is full
            if len(data_to_write) >= 25000:

                # check if there are data from different years
                years = data_to_write["timestamp"].dt.year.unique().tolist()
                #print(f"Data from these years {years}")
                
                # Iterate over the year values in data_to_write and write the data to DB by years
                for year in years:
                    write_df_to_db(
                        db_conn=db_conn,
                        data=data_to_write[data_to_write["timestamp"].dt.year == year],
                        table_name=f"index_data_{year}",
                        print_msg= False #f"{index_name}-{year}"
                        )
                
                # Empty collector
                delete_df(data_to_write)
                #data_to_write = pd.DataFrame()

        #print(f"{index_name} last date: {last_timestamp}")
        #print(f"data to write size: {data_to_write.shape}\n")

    # Write any remining data in the collector
    # check if there are data from different years
    years = data_to_write["timestamp"].dt.year.unique().tolist()
    #print(f"Data from these years {years}")
    
    # Iterate over the year values in data_to_write and write the data to DB by years
    for year in years:
        write_df_to_db(
            db_conn=db_conn,
            data=data_to_write[data_to_write["timestamp"].dt.year == year],
            table_name=f"index_data_{year}",
            print_msg= False #f"{index_name}-{year}")
            )
    
    #print(f"{index_name} last date: {last_timestamp}")
    #print(f"data to write size: {data_to_write.shape}\n")

    delete_df(data_to_write)
