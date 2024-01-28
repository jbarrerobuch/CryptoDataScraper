import Agent as Agent
from variables import *
import datetime

collector = Agent.Collector()

collector.init_deribit(
    apiKey=deribit_apiKey,
    apisecret=deribit_apisecret,
    sandbox_mode=False
)
collector.init_db_conn(
    db_name=db_name,
    db_user=db_user,
    db_password=db_password
)

# Query instruments in data base
query = collector.read_last_date_from_instruments(is_active=True).sort_values("expiration_timestamp", ascending=True).reset_index(drop=True).loc[0,:]
print(query)
#data_test = collector.fetch_ohlcv(
#    instrument_name=query["instrument_name"],
#    instrument_id=query["instrument_id"],
#    start_timeframe=query["creation_timestamp"],
#    end_timeframe=query["expiration_timestamp"]
#)

data_test = collector.fetch_ohlcv(
    instrument_name=query["instrument_name"],
    instrument_id=query["instrument_id"],
    start_timeframe=query["max_timestamp"] + datetime.timedelta(minutes=1),
    end_timeframe=query["expiration_timestamp"]
)
print(data_test)
#write_in_db = collector.write_df_to_db(data=data_test, table_name="market_data")
#print("Succesful write in db: ", write_in_db)
#print(f"Rows {data_test.shape()}")
#collector.write_marketdata_in_db()