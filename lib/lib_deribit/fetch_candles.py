import datetime as dt
import polars as pl
import ccxt
from ..Agent import Collector

__all__ = ["fetch_candles"]

def fetch_candles(agent:Collector, instrument_name:str, instrument_id:str, start_timeframe:dt.datetime, end_timeframe:dt.datetime=dt.datetime.now(tz=dt.timezone.utc), resolution:int=1) -> pl.DataFrame:
        """
        Fetches market data candles for a given instrument from Deribit.

        **Parameters:**\n
        agent (Collector): The Collector object used to interact with Deribit.
        instrument_name (str): The name of the instrument to fetch data for.
        instrument_id (str): The ID of the instrument to fetch data for.
        start_timeframe (dt.datetime): The start date and time of the data range to fetch.
        end_timeframe (dt.datetime): The end date and time of the data range to fetch (default: current UTC time).
        resolution (int): The time frame aggregation in minutes (default: 1).

        **Returns:**\n
        pl.DataFrame: A Polars DataFrame containing the fetched market data candles.
        """
        # need to catch Err for instrument not found
        # Exception has occurred: BadRequest       
        # (note: full exception trace is shown but execution is paused at: <module>)
        # deribit {"usOut":1703446335056501,"usIn":1703446335056492,"usDiff":9,"testnet":false,"jsonrpc":"2.0","error":{"message":"Invalid params","data":{"reason":"instrument not found","param":"instrument_name"},"code":-32602}}
        #
        try:
            candles = agent.deribit.public_get_get_tradingview_chart_data(
                params={
                    "instrument_name": instrument_name,
                    "start_timestamp": int(start_timeframe.timestamp())*1000,
                    "end_timestamp": int(end_timeframe.timestamp())*1000,
                    "resolution": 1
                }
            )["result"] # Candle data is in result key from server answer
        

        except ccxt.BadRequest as e:
            print(f"----BadRequest: {e}")
            #sql = f"UPDATE instruments SET not_found = '{dt.datetime.today().strftime('%Y-%m-%d')}', is_active = false WHERE instrument_name = '{instrument_name}'"
            #db_methods.execute_sql(query=sql)
            df = pl.DataFrame()
            return df
        
        except ccxt.OnMaintenance as e:
            print(f"----OnMaintenance: {e}")
            return df

        except ccxt.RequestTimeout as e:
            print(f"----RequestTimeout: {e}")
            return df
        

        else:
            #print(candles)
            if candles["status"] == "no_data":
                print(f"Instrument: {instrument_name} - no data")
                df = pl.DataFrame()
            elif candles["status"] == "ok":
                candles["timestamp"] = candles["ticks"] # rename ticks to timestamp
                del candles["status"] # Drop no needed keys
                del candles["ticks"] # Drop no needed keys

                # transform the dictionary to df
                df = pl.DataFrame(
                    data=candles
                )
                df = df.with_columns_seq(
                    [
                        pl.lit(instrument_name).alias("instrument_name"),
                        pl.lit(instrument_id).alias("instrument_id"),
                        pl.col("timestamp").cast(pl.Int64).cast(pl.Datetime("ms")).alias("timestamp"),
                        pl.concat_str(
                            [
                                pl.lit(instrument_id),
                                pl.col("timestamp").cast(pl.Utf8)
                            ],
                            separator="-"
                        ).alias("data_id"),
                        pl.lit("Deribit").alias("exchange")
                    ]
                )
            
            return df