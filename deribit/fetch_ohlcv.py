import datetime as dt
import pandas as pd
import ccxt
import db_methods.execute_sql

def fetch_ohlcv(deribit_obj, instrument_name:str, instrument_id:str, start_timeframe:dt.datetime, end_timeframe:dt.datetime=dt.datetime.now(tz=dt.timezone.utc), resolution:int=1) -> pd.DataFrame:
        """
        Fetch of market data candles Open, high, low, close and volume.\n
        - symbol: intrument name or asset symbol i.e. 'ETH-21DEC23-1800-C'
        - start_timeframe and end_timeframe: int, unxi epoch timestamp in miliseconds\n
        - resolution: int, data candle timefram aggrgation in minutes. [ 1, 3, 5, 10, 15, 30, 60, 120, 180, 360, 720]

        It returns the data in a dataframe.
        """
        # need to catch Err for instrument not found
        # Exception has occurred: BadRequest       
        # (note: full exception trace is shown but execution is paused at: <module>)
        # deribit {"usOut":1703446335056501,"usIn":1703446335056492,"usDiff":9,"testnet":false,"jsonrpc":"2.0","error":{"message":"Invalid params","data":{"reason":"instrument not found","param":"instrument_name"},"code":-32602}}
        #
        try:
            candles = deribit_obj.public_get_get_tradingview_chart_data(
                params={
                    "instrument_name": instrument_name,
                    "start_timestamp": int(start_timeframe.timestamp())*1000,
                    "end_timestamp": int(end_timeframe.timestamp())*1000,
                    "resolution": 1
                }
            )["result"] # Candle data is in result key from server answer
        

        except ccxt.BadRequest as e:
            print(f"----BadRequest: {e}")
            sql = f"UPDATE instruments SET not_found = '{dt.datetime.today().strftime('%Y-%m-%d')}', is_active = false WHERE instrument_name = '{instrument_name}'"
            db_methods.execute_sql(query=sql)
            df = pd.DataFrame()
        

        else:
            #print(candles)
            if candles["status"] == "no_data":
                print(f"Instrument: {instrument_name} - no data")
                df = pd.DataFrame()
            elif candles["status"] == "ok":
                candles["timestamp"] = candles["ticks"] # rename ticks to timestamp
                del candles["status"] # Drop no needed keys
                del candles["ticks"] # Drop no needed keys

                # transform the dictionary to df
                df = pd.DataFrame(candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                df["instrument_name"] = instrument_name
                df["instrument_id"] = instrument_id
                df["data_id"] = pd.concat([df["instrument_id"], df["timestamp"]], axis=1,).apply(lambda row: "-".join(row), axis=1)
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                #print(f"Instrument: {instrument_name} - {len(df)} data points")


        finally:
            return df