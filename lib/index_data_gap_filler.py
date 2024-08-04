import lib.Agent as Agent
import json
from variables import *
import datetime as dt
import time
import pandas as pd

collector = Agent.Collector()

#collector.init_deribit(
#    apiKey=deribit_apiKey,
#    apisecret=deribit_apisecret,
#    sandbox_mode=False
#)
#collector.init_binance(
#    apiKey=binance_apiKey,
#    apisecret=binance_apisecret,
#    sandbox_mode=False
#)
collector.init_db_conn(
    db_name=db_name,
    db_user=db_user,
    db_password=db_password
)

def index_data_gap_filler(index_name:str, year:int) -> list:
    """
    Reads the DB for the selected index_name and year and returns results\n
    ordered by timestamp ascending. Finds gaps in the data and returns a list of dictionaries with timestamps:
    i.e.
    return = [
        {'start': Timestamp('2020-01-19 13:09:00'), 'end': Timestamp('2020-01-19 13:37:00')},
        {'start': Timestamp('2020-12-17 07:32:00'), 'end': Timestamp('2020-12-17 07:55:00')}
        ]
    """

    # Query to read DB
    query = f"SELECT * FROM index_data_{year} WHERE index_name = '{index_name}' ORDER BY timestamp ASC"

    data = collector.execute_sql(query=query)
    #with collector.conn.cursor() as cursor:
    #    cursor.execute(sql=sql)
    #    data = cursor.fetchall()

    df = pd.DataFrame().from_records(
        data=data,
        columns=[
            "data_id",
            "index_name",
            "exchange",
            "timestamp",
            "open",
            "high",
            "low",
            "close"
            ])

    t_diff = df["timestamp"].diff()
    diff_1min = t_diff <= pd.Timedelta(minutes=1)
    index_gaps = df.loc[diff_1min == False, "timestamp"]

    print(index_gaps)
    result = []
    print("--------Ranges-----------")
    for i, value in index_gaps.items():
        print(i, value)
        missing_range = {
            "start": df["timestamp"].iloc[i-1] + dt.timedelta(minutes=1),
            "end": df["timestamp"].iloc[i] - dt.timedelta(minutes=1)
            }
        if i == 0:
            missing_range["start"] = dt.datetime(year=2020, month=1, day=1, hour=0, minute=0)
        result.append(missing_range)


    print(result)

if __name__ == "__main__":
    index_data_gap_filler("ADAUSDT", 2020)
