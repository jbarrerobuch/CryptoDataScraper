import Agent as Agent
import json
from variables import *
import datetime as dt
import time
import pandas as pd

collector = Agent.Collector()

#collector.init_deribit(
#    deribit_apiKey=deribit_apiKey,
#    deribit_apisecret=deribit_apisecret,
#    sandbox_mode=False
#)
collector.init_binance(
    apiKey=binance_apiKey,
    apisecret=binance_apisecret,
    sandbox_mode=False
)
collector.init_db_conn(
    db_name=db_name,
    db_user=db_user,
    db_password=db_password
)

index_list = [
    "ETHUSDT",
    "XRPUSDT",
    "LTCUSDT",
    "ADAUSDT",
    "DOGEUSDT",
    "SOLUSDT",
    "MATICUSDT",
    "BTCUSDT"
]

for i in index_list:
    collector.write_index_data(
        index_name=i,
        interval="1m",
        start_timestamp=dt.datetime(year=2020, month=1, day=1, hour=0, minute=0),
    )

"""start_timestamp = dt.datetime(year=2021, month=1,day=1, hour=0, minute=0, second=0)
end_timestamp = dt.datetime(year=2021, month=12,day=31, hour=23, minute=59, second=0)
print(f"Start collecting since {start_timestamp}")
print()
last_date = start_timestamp
data_to_write = pd.DataFrame()

while last_date < end_timestamp:

    # retrive data from the last available data in the DB or from the startTimestamp defined
    index_data = collector.get_index_data(
        index_name="BTCUSDT",
        interval="1m",
        start_timestamp=last_date,
        )
    
    index_data = index_data[index_data["timestamp"].dt.year == start_timestamp.year]
    last_date = index_data["timestamp"].max()

    # Append retrived data to bulk writer
    if data_to_write.empty:
        data_to_write = index_data
    else:
        data_to_write = pd.concat([data_to_write, index_data])
    
    # Write to the DB if the bulk container is full
    if len(data_to_write) >= 25000:
        collector.write_df_to_db(data=data_to_write, table_name=f"index_data_{start_timestamp.year}", verbose=True)
        data_to_write = pd.DataFrame()
    
    print(f"last date: {last_date}")
    print(f"data to write size: {data_to_write.shape}\n")

collector.write_df_to_db(data=data_to_write, table_name=f"index_data_{start_timestamp.year}", verbose=True)"""
