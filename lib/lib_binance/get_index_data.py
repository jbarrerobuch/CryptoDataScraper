import datetime as dt
import polars as pl
import ccxt

__all__ = ["get_index_data"]

def get_index_data(binance_obj:ccxt.Exchange, index_name:str, interval:str="1m", start_timestamp:dt.datetime=None, exhange:str="binance") -> pl.DataFrame:
        """
        Retrive 500 intervals of asset data from Binance API
        - pair (str): required. Trading pair i.e. BTCUSDT
        - interval (str): required. Time interval ["1m", "1h", "4h", "1D"...]
        - startTime (int): unix epoch timestamp in miliseconds.

        If startTime is not sent, the most recent klines are returned.
        """
        
        # Convert datetime object to unix EPOCH miliseconds
        if start_timestamp is not None:
            start_timestamp.replace(tzinfo=dt.timezone.utc)
            startTime_unix = int(start_timestamp.timestamp()*1000)
        else:
            startTime_unix = start_timestamp

        # Collect data
        try:
            data = binance_obj.fetch_ohlcv(
                symbol=index_name,
                timeframe=interval,
                since=startTime_unix,
                params={"price": "index"})
        
        except Exception as e:
            print(f"Error: {e}")
            df = pl.DataFrame()

        else:

            if len(data) == 0:
                df = pl.DataFrame()
            
            else:

                # Dataframe data results
                df = pl.DataFrame(
                    data=data,
                    schema=["timestamp", "open", "high",  "low", "close", "vol"]
                )

                # Insert Categorical data, data_id and convert unix EPOCH timsestamps to datetime object
                df = df.with_columns(
                    [
                        pl.lit(index_name).alias("index_name"),
                        pl.lit(exhange).alias("exchange")
                    ]
                )
                df = df.with_columns(
                     [pl.concat_str(
                            [
                                pl.col("exchange"),
                                pl.col("index_name"),
                                pl.col("timestamp").cast(pl.Utf8)
                            ],
                            separator="-").alias("data_id"),
                        pl.col("timestamp").cast(pl.Datetime).dt.with_time_unit("ms")]
                )

        return df