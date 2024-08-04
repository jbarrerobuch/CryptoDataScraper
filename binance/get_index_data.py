import datetime as dt
import pandas as pd

def get_index_data(binance_obj, index_name:str, interval:str="1m", start_timestamp:dt.datetime=None):
        """
        Retrive 500 intervals of asset data from Binance API
        - pair (str): required. Trading pair i.e. BTCUSDT
        - interval (str): required. Time interval ["1m", "1h", "4h", "1D"...]
        - startTime (int): unix epoch timestamp in miliseconds.

        If startTime is not sent, the most recent klines are returned.
        """

        # Convert datetime object to unix EPOCH miliseconds
        if start_timestamp == None:
            startTime_unix = None
        else:
            start_timestamp.replace(tzinfo=dt.timezone.utc)
            startTime_unix = int(start_timestamp.timestamp()*1000)

        # Collect data
        data = binance_obj.fetch_ohlcv(
            symbol=index_name,
            timeframe=interval,
            since=startTime_unix,
            params={"price": "index"})

        # Dataframe data results
        df = pd.DataFrame().from_records(
            data=data,
            columns=["timestamp", "open", "high",  "low", "close", "vol"]
        )
        df.drop(columns=["vol"], inplace=True)

        # Insert Categorical data, data_id and convert unix EPOCH timsestamps to datetime object
        df["index_name"] = index_name
        df["exchange"] = "binance"
        df["data_id"] = pd.concat([df["exchange"], df["index_name"], df["timestamp"].astype(str)], axis=1,).apply(lambda row: "-".join(row), axis=1)
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")

        return df