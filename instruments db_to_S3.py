import datetime as dt
import time
from lib import *
import memory_profiler as mp
import pandas as pd


if __name__ == "__main__":
    
    # Index list to retrieve
    index_map = {
        "ETHUSDT": "eth_usdt",
        "XRPUSDT": "xrp_usdt",
        "LTCUSDT": "ltc_usdt",
        "ADAUSDT": "ada_usdt",
        "DOGEUSDT": "doge_usdt",
        "SOLUSDT": "sol_usdt",
        "MATICUSDT": "matic_usdt",
        "BTCUSDT": "btc_usdt"
        }
    
    index_list = list(index_map.keys())

    # Define AWS vars
    table = "market_data"
    path = "s3://scraped-cryptodata"
    bucket_name = "scraped-cryptodata"
    storage_options = {
        'anon': False
    }

    agent = init_agent(db=True, deribit=False, binance=False)
    
    # Parquert partitioning
    partition_cols = ["p-exchange","p-price_index", "p-year", "p-instrument_type"]
    
    # Rename columns of Index data to match instrument market_data
    col_maping = {
        "index_name": "instrument_name"
    }

    # Years to retrieve
    years =[2018, 2019, 2020, 2021, 2022]

    for year in years:
        for key, value in index_map.items():
            print(f"Getting data from {key}/{value} for year {year}")

            # Get data from market_data
            #print(f"Getting data from market_data for year {year}")
            #market_data = db_methods.execute_sql(
            #    db_conn=agent.conn,
            #    output="df",
            #query = f"""SELECT 
            #    m.*, 
            #    i.price_index,
            #    EXTRACT(YEAR FROM timestamp) AS year
            #    FROM market_data m
            #    JOIN instruments i ON m.instrument_id = i.instrument_id
            #    WHERE EXTRACT(YEAR FROM timestamp) = {year};
            #    """
            #)
            #if len(market_data) == 0:
            #    print("No market data found")
            #    print("Data transform to append")
            #    # Adding columns to match index data
            #    market_data["exchange"] = "deribit"
            #    market_data["instrument_type"] = "option"


            #    market_data["p-exchange"] = "deribit"
            #    market_data["p-instrument_type"] = "option"

            # Get data from index_data_{year}
            print(f"Getting data from index_data_{year}")
            index_data = lib_db.execute_sql(
                db_conn=agent.conn,
                output="df",
                query = f"""SELECT *
                FROM index_data_{year}
                WHERE index_name = '{key}';"""
            )

            # Add and rename columns to match market_data
            print("Data transform index_data to append")
            index_data["volume"] = 0
            index_data["instrument_id"] = index_data["exchange"] + "-" + index_data["index_name"]
            index_data["instrument_type"] = "index"
            index_data["price_index"] = index_data["index_name"].map(index_map)
            index_data["year"] = year
            index_data.rename(columns=col_maping, inplace=True)

            print("Appending data and definde data types")
            #data = pd.concat([market_data, index_data], axis=0)
            data = index_data
            data["instrument_id"] = data["instrument_id"].astype(str)
            data["data_id"] = data["data_id"].astype(str)
            data["volume"] = data["volume"].astype(float)
            data["price_index"] = data["price_index"].astype(str)

            # Add partion cols
            data["p-exchange"] = data["exchange"]
            data["p-price_index"] = data["price_index"]
            data["p-year"] = data["year"]
            data["p-instrument_type"] = data["instrument_type"]


            # Write data to parquet
            print(f"Writing data {key}/{value} for year {year} to S3")    
            lib_db.write_df_to_parquet(
                data=data,
                output_path=f"{path}/{table}/{year}-{dt.datetime.now()}",
                partition_cols=partition_cols,
                storage_options=storage_options
            )

            print(data.shape)