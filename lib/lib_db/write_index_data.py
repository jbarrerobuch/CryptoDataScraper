import datetime as dt
import pandas as pd
from .check_index_data_year_complete import check_index_data_year_complete
from .write_df_to_db import write_df_to_db
from ..lib_binance import *
from ..delete_df import delete_df
from ..Agent import Collector

__all__ = ["write_index_data"]

def write_index_data(agent:Collector, index_name:str, interval:str, start_timestamp:dt.datetime, end_timestamp:dt.datetime=dt.datetime.now(dt.timezone.utc), exchange:str="binance", output_path:str=None):
    """
	Retrieves and writes index data from Binance to a specified database.

	**Parameters:**
	agent (Collector): The collector object used to interact with the database.
	index_name (str): The name of the index to retrieve data for.
	interval (str): The interval at which to retrieve data. Valid intervals are:
		- 1s
		- 1m
		- 3m
		- 5m
		- 15m
		- 30m
		- 1h
		- 2h
		- 4h
		- 6h
		- 8h
		- 12h
		- 1d
		- 3d
		- 1w
		- 1M
	start_timestamp (dt.datetime): The start timestamp for which to retrieve data.
	end_timestamp (dt.datetime): The end timestamp for which to retrieve data. Defaults to the current timestamp.
	exchange (str): The exchange to retrieve data from. Defaults to "binance".
	output_path (str): The output path for the data. Defaults to None.

	**Returns:**
	None
	"""

    #print(f"Start collecting {index_name} since {start_timestamp} to {end_timestamp}")
    #print()
    #last_timestamp = check_index_data_year_complete(
    #    db_conn=db_conn,
    #    index_name=index_name,
    #    year=start_timestamp.year,
    #    exchange="binance"
    #    )
    
    #print(f"Last timestamp in DB is {last_timestamp}")

    last_timestamp = start_timestamp
    # Check valid intervals
    valid_intervals = ['1s', '1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1M']
    
    if not interval in valid_intervals:
        raise ValueError(f"Invalid interval. Valid intervals are: {valid_intervals}")

    # Initialize data collector for bulk writing in DB
    data_to_write = pd.DataFrame()

    # Refresh end timestamp to now
    end_timestamp = dt.datetime.now(dt.timezone.utc)

    # Loop until end timestamp is reached
    while last_timestamp < end_timestamp:

        # retrive data from the last available data in the DB or from the startTimestamp defined
        index_data = get_index_data(
            binance_obj=agent.binance,
            index_name=index_name,
            interval=interval,
            start_timestamp=last_timestamp
            )

        if index_data.empty:
            print(f"Returned empty getting data from:\n\ {index_name} - {last_timestamp}")
        else:
            # Append retrived data to bulk writer
            if data_to_write.empty:
                data_to_write = index_data
            else:
                data_to_write = pd.concat([data_to_write, index_data], ignore_index=True)
                delete_df(index_data)

            # Write to the DB if the bulk container is full
            if len(data_to_write) >= 25000:

                # Write data to DB PostgreSQL
                if agent.db_type == "postgres":
                    # check if there are data from different years
                    years = data_to_write["timestamp"].dt.year.unique().tolist()
                    
                    # Iterate over the year values in data_to_write and write the data to DB by years
                    for year in years:
                        write_df_to_db(
                            agent=agent,
                            data=data_to_write[data_to_write["timestamp"].dt.year == year],
                            table_name=f"index_data_{year}"
                            )

                # Write data to DB Athena
                elif agent.db_type == "athena":
                    data_to_write['price_index'] = data_to_write['index_name'].str.lower().str.replace('usdt', '_usd')
                    data_to_write["instrument_type"] = "index"
                    write_df_to_db(
                        agent=agent,
                        data=data_to_write,
                        table_name="market_data",
                        output_path=output_path
                        )
                
                # Empty collector
                delete_df(data_to_write)

    # Write any remining data in the collector
    
    # Write data to DB PostgreSQL
    if agent.db_type == "postgres":
        # check if there are data from different years
        years = data_to_write["timestamp"].dt.year.unique().tolist()
        
        # Iterate over the year values in data_to_write and write the data to DB by years
        for year in years:
            write_df_to_db(
                agent=agent,
                data=data_to_write[data_to_write["timestamp"].dt.year == year],
                table_name=f"index_data_{year}"
                )

    # Write data to DB Athena
    elif agent.db_type == "athena":
        data_to_write['price_index'] = data_to_write['index_name'].str.lower().str.replace('usdt', '_usd')
        data_to_write["instrument_type"] = "index"
        write_df_to_db(
            agent=agent,
            data=data_to_write,
            table_name="market_data",
            output_path=output_path
            )
    
    # Empty collector
    delete_df(data_to_write)
