import ccxt
import pandas as pd
import datetime as dt
import time
from pytz import timezone, all_timezones
import os
import psycopg2
import json

class Collector:

    def __init__(self) -> None:
        self.deribit = None
        self.binance = None
        self.conn = None
        self.cur = None
        self.stats = {
            "datapoints": {"writen": 0, "failed": 0},
            "running_time": dt.timedelta(seconds=0),
            "iterations": 0
        }


    # Connector to APIs and DB

    def init_deribit(self, apiKey:str="", apisecret:str="", enableRateLimit:bool=True, sandbox_mode:bool=False):
        self.deribit = ccxt.deribit(
            {
            "apiKey": apiKey,
            "secret": apisecret,
            "enableRateLimit": enableRateLimit
            }
        )
        self.deribit.set_sandbox_mode(sandbox_mode)
        self.deribit.load_markets()
        print("deribit initialized")
        print(self.deribit.fetch_status())
        print()


    def init_binance(self, apiKey:str, apisecret:str, enableRateLimit:bool=True, sandbox_mode:bool=False):
        self.binance = ccxt.binance(
            {
            "apiKey": apiKey,
            "secret": apisecret,
            "enableRateLimit": enableRateLimit
            }
        )
        self.binance.set_sandbox_mode(sandbox_mode)
        self.binance.load_markets()
        print("binance initialized")
        print(self.binance.fetch_status())
        print()


    def init_db_conn(self, db_name:str, db_user:str, db_password:str):
        self.conn = psycopg2.connect(f"dbname={db_name} user={db_user} password={db_password}")
        self.cur = self.conn.cursor()
        self.cur.execute('SELECT version()')
        db_version = self.cur.fetchone()
        self.cur.close()

        print("version():", db_version)
        print("connection status:", self.conn.status)
        print()
    

    # DB methods

    def execute_sql(self,query:str):
        #print(f"SQL to execute:\n{sql}")
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query)
                if query.startswith("SELECT"):
                    return cursor.fetchall()
                else:
                    #qresult = cursor.rowcount
                    self.conn.commit()
            print("Execute SQL succesful")
        except Exception as err:
            print(f"Exception raised:{err}")
            self.conn.rollback()


    def write_df_to_db(self, data:pd.DataFrame, table_name:str, print_msg:str=None, verbose:str="False") -> bool:
        """
        Bulk write a dataframe to a table.
        Required:
        - Dataframe's column names match DB_table's columns names
        - Default index. It will be reset before writing
        """
        # Reset dataframe's index to iterate over it
        data.reset_index(drop=True, inplace=True)
        # Convert values from dataframe to dictionary
        status = False
        values = data.to_dict(orient="index")

        if print_msg:
            print("================")
            print(f"Writing {len(data)} data points.")
            print(print_msg)
        else:
            print("================")
            print(f"Writing {len(data)} data points to DB...")
        
        # Gather column names and concatenate them comma separated
        col_names = data.columns.to_list()
        col_names_csv = ",".join(col_names)

        # Map column name in dataframe to same name column in the DB
        col_names_map = ",".join([f"%({i})s" for i in col_names])

        # Build the argument value to execute the query
        arg = f"INSERT INTO {table_name} ({col_names_csv}) VALUES ({col_names_map})"
        writen_rows = 0
        writen_failed = 0
        # Writing each row by iterating the value list
        with self.conn.cursor() as cursor:
            status = False
            for i in range(len(values)):
                #print(f"values{i}")
                #print(values[i])
                #print()
                try:
                    cursor.execute(arg, values[i])
                    self.conn.commit()
                    status = True
                    writen_rows += 1
                except Exception as e:
                    if verbose == "True":
                        print(f"Writing data failed... Rollback - Error:\n values{i} - {e}")
                    self.conn.rollback()
                    writen_failed += 1
        
        print("================")
        print(f"DB Writing ended\nWriten: {writen_rows}\nFailed: {writen_failed}\nTotal: {writen_rows+writen_failed}")
        print("================")
        print()
        if table_name == "market_data":
            self.stats["datapoints"]["writen"] += writen_rows
            self.stats["datapoints"]["failed"] += writen_failed

        # Return writing status
        return status


    # Options from Deribit

    def get_currency_list(self) -> list:
        currencies_raw = self.deribit.publicGetGetCurrencies() # type: ignore
        currency_list = []
        for currency in currencies_raw["result"]:
            currency_list.append(currency["currency"])
        return currency_list
    

    def get_instruments(self, currency_list:list, kind="option", expired = False) -> pd.DataFrame:
        dtypes = {
                "tick_size_steps": "str",
                "quote_currency": "str",
                "min_trade_amount": "int64",
                "expiration_timestamp": "datetime64",
                "counter_currency": "str",
                "settlement_currency": "str",
                "block_trade_tick_size": "float64",
                "block_trade_min_trade_amount": "float64",
                "block_trade_commission": "float64",
                "option_type": "str",
                "settlement_period": "str",
                "creation_timestamp": "datetime64",
                "contract_size": "float64",
                "base_currency": "str",
                "instrument_id": "str",
                "instrument_type": "str",
                "taker_commission": "float64",
                "maker_commission": "float64",
                "tick_size": "float64",
                "strike": "float64",
                "is_active": "bool",
                "instrument_name": "str",
                "kind": "str",
                "rfq": "bool",
                "price_index": "str",
                "is_complete": "bool"
        }
        instruments_df = pd.DataFrame(columns = dtypes.keys()).astype(dtypes)
        for currency in currency_list:
            instruments_raw = self.deribit.publicGetGetInstruments(
                params={
                    "currency": currency, # type: ignore
                    "kind": kind,
                    "expired": expired
                }
            )
            data = pd.DataFrame.from_records(instruments_raw["result"])
            instruments_df = pd.concat([instruments_df, data])
            instruments_df.reset_index(drop=True, inplace=True)


        instruments_df['expiration_timestamp'] = pd.to_datetime(instruments_df['expiration_timestamp'], unit="ms", utc= True)
        instruments_df['creation_timestamp'] = pd.to_datetime(instruments_df['creation_timestamp'], unit="ms", utc= True)
        return instruments_df


    def write_instruments_table(self, is_active=True):
        currencies = self.get_currency_list()
        print(f"Updating instruments with active status {is_active}")

        instruments = self.get_instruments(currency_list=currencies, expired=not is_active)
        instruments["is_complete"] = False
        instruments["tick_size_steps"] = instruments["tick_size_steps"].apply(json.dumps)
        #print(instruments.head())
        self.write_df_to_db(data=instruments, table_name="instruments", verbose="False")
        #print(write_in_db)


    def write_instruments_status(self) -> None:

        # SET EXPIRED INSTRUMENTS TO is_active = False
        sql = "UPDATE instruments SET is_active = false\n\
            WHERE expiration_timestamp < CURRENT_DATE;"
        self.execute_sql(query=sql)
        print("Updated expiration statuses")

        # SET COMPLETE INSTRUMENTS TO is_complete = True
        sql = """
            UPDATE instruments
            SET is_complete = TRUE
            FROM (
                SELECT
                    instruments.instrument_name,
                    instruments.creation_timestamp as creation_timestamp,
                    min(timestamp) as min_timestamp,
                    instruments.expiration_timestamp as expiration_timestamp,
                    max(timestamp) as max_timestamp
                FROM market_data
                INNER JOIN instruments
                ON market_data.instrument_id = instruments.instrument_id
                GROUP BY instruments.instrument_name, instruments.creation_timestamp, instruments.expiration_timestamp
                HAVING instruments.expiration_timestamp <= max(market_data.timestamp)
            ) AS subquery
            WHERE instruments.instrument_name = subquery.instrument_name;
        """
        self.execute_sql(query=sql)
        print("Updated complete statuses")


    def read_instruments_from_db(self, is_active:bool=True, is_complete:bool=False) -> pd.DataFrame:

        """
        Fetch columns from the table "instruments" filtered by:
        - is_active (bool): default True
        - is_complete (bool): default False

        SQL SNIPPET
        ------------------------------
        SELECT * FROM instruments\n
        WHERE is_active = {is_active} AND is_complete = {is_complete}
        """
        # Build SQL snippets for execution

        sql = f"SELECT * FROM instruments WHERE is_active = {is_active} AND is_complete = {is_complete}"

        #print(f"SQL to execute:\n {sql}")

        # initialize the cursor and execute the SQL sentence
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(sql)
                qresult = cursor.fetchall()
            self.conn.commit()
        except Exception as err:
            print(f"Exception raised:{err}")
            self.conn.rollback()

        # Define dfresult and dtypes
        dtypes = {
            "tick_size_steps": "str",
            "quote_currency": "str",
            "min_trade_amount": "int64",
            "expiration_timestamp": "datetime64",
            "counter_currency": "str",
            "settlement_currency": "str",
            "block_trade_tick_size": "float64",
            "block_trade_min_trade_amount": "float64",
            "block_trade_commission": "float64",
            "option_type": "str",
            "settlement_period": "str",
            "creation_timestamp": "datetime64",
            "contract_size": "float64",
            "base_currency": "str",
            "instrument_id": "str",
            "instrument_type": "str",
            "taker_commission": "float64",
            "maker_commission": "float64",
            "tick_size": "float64",
            "strike": "float64",
            "is_active": "bool",
            "instrument_name": "str",
            "kind": "str",
            "rfq": "bool",
            "price_index": "str",
            "is_complete": "bool",
            "not_found": "datetime64"
        }

        #print("Results in query",len(qresult))
        dfresult = pd.DataFrame(qresult, columns=[desc[0] for desc in cursor.description]).astype(dtypes)
        #print(dfresult.info())

        return dfresult
    

    def read_last_date_from_instruments(self, is_active:bool=True, is_complete:bool=False, get_last_data_timestamp = False) -> pd.DataFrame:
        """
        Fetch timestamp columns from the table "instruments" filtered by:
        - is_active (bool): default True
        - is_complete (bool): default False
        - get_last_data_timestamp (bool): defaul False; get last market data timestamp

        and the first (if get_min_date = True) and last timestamp available in market_data

        SQL SNIPPET
        ------------------------------
        SELECT instruments.instrument_name, instruments.creation_timestamp as creation_timestamp, min(timestamp) as min_timestamp,
            instruments.expiration_timestamp as expiration_timestamp, max(timestamp) as max_timestamp
        FROM market_data
        INNER JOIN instruments
        ON market_data.instrument_id = instruments.instrument_id
        GROUP BY instruments.instrument_name, instruments.creation_timestamp, instruments.expiration_timestamp
        """
        # Build SQL snippets for execution

        if get_last_data_timestamp:
            sql = f"SELECT instruments.instrument_name, instruments.instrument_id, instruments.is_active,\
                instruments.creation_timestamp as creation_timestamp, min(timestamp) as min_timestamp,\
                instruments.expiration_timestamp as expiration_timestamp,\
                max(timestamp) as max_timestamp\
                FROM market_data\
                INNER JOIN instruments ON market_data.instrument_id = instruments.instrument_id\
                WHERE instruments.is_active = {is_active} AND is_complete = {is_complete}\
                GROUP BY instruments.instrument_name, instruments.instrument_id, instruments.is_active, instruments.creation_timestamp, instruments.expiration_timestamp\
                ORDER BY instruments.expiration_timestamp DESC"
            # Define dfresult and dtypes
            dtypes = {
                "instrument_name": "str",
                "instrument_id": "str",
                "is_active": "bool",
                "creation_timestamp": "datetime64",
                "min_timestamp": "datetime64",
                "expiration_timestamp": "datetime64",
                "max_timestamp": "datetime64"
            }

        else:
            sql = f"SELECT instruments.instrument_name, instruments.instrument_id, instruments.is_active,\
                instruments.creation_timestamp as creation_timestamp, instruments.expiration_timestamp as expiration_timestamp,\
                max(timestamp) as max_timestamp\
                FROM market_data\
                INNER JOIN instruments ON market_data.instrument_id = instruments.instrument_id\
                WHERE instruments.is_active = {is_active} AND is_complete = {is_complete}\
                GROUP BY instruments.instrument_name, instruments.instrument_id, instruments.is_active, instruments.creation_timestamp, instruments.expiration_timestamp\
                ORDER BY instruments.expiration_timestamp DESC"
            # Define dfresult and dtypes
            dtypes = {
                "instrument_name": "str",
                "instrument_id": "str",
                "is_active": "bool",
                "creation_timestamp": "datetime64",
                "expiration_timestamp": "datetime64",
                "max_timestamp": "datetime64"
            }

        #print(f"SQL to execute:\n {sql}")

        # initialize the cursor and execute the SQL sentence
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(sql)
                qresult = cursor.fetchall()
            self.conn.commit()
        except Exception as err:
            print(f"Exception raised:{err}")
            self.conn.rollback()

        #print("Results in query",len(qresult))
        dfresult = pd.DataFrame(qresult, columns=[desc[0] for desc in cursor.description]).astype(dtypes)
        #print(dfresult.info())

        return dfresult


    def fetch_ohlcv(self, instrument_name:str, instrument_id:str, start_timeframe:dt.datetime, end_timeframe:dt.datetime=dt.datetime.now(tz=dt.timezone.utc), resolution:int=1) -> pd.DataFrame:
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
            candles = self.deribit.public_get_get_tradingview_chart_data(
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
            self.execute_sql(query=sql)
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


    def write_marketdata_in_db(self, is_active:bool=True, is_complete:bool=False) -> None:
        """
        Gathers instrument data from the DB including the last datapoint's timestamps and gets market data of each from the Exchange.
        Then writes all the market data in the DB.
        """
        # Query instruments in data base
        data_to_write = pd.DataFrame()
        instrument_count = 0
        instruments = self.read_last_date_from_instruments(is_active=is_active, is_complete=is_complete)
        for index,row in instruments.iterrows():
            if row["max_timestamp"]:
                #print(row)
                market_data = self.fetch_ohlcv(
                    instrument_name=row["instrument_name"],
                    instrument_id=row["instrument_id"],
                    start_timeframe=row["max_timestamp"] + dt.timedelta(minutes=1),
                    end_timeframe=row["expiration_timestamp"]
                )
            else:
                #print(row)
                market_data = self.fetch_ohlcv(
                    instrument_name=row["instrument_name"],
                    instrument_id=row["instrument_id"],
                    start_timeframe=row["creation_timestamp"],
                    end_timeframe=row["expiration_timestamp"]
                )
            if market_data.empty:
                print(f"Return empty get market data\n\
                      {row['instrument_name']} from {row['max_timestamp']}")

            if not market_data.empty:
                if data_to_write.empty:
                    data_to_write = market_data
                else:
                    data_to_write = pd.concat([data_to_write, market_data], ignore_index=True)
                instrument_count += 1

            if instrument_count >= 100 and len(data_to_write) > 0:
                data_to_write.reset_index(inplace=True, drop=True)
                self.write_df_to_db(data=data_to_write, table_name="market_data", print_msg=f"{data_to_write['instrument_name'].unique()}")
                data_to_write = pd.DataFrame()
                #print("Succesful write in db: ", write_in_db)
                instrument_count = 0
        if not data_to_write.empty:
            self.write_df_to_db(data=data_to_write, table_name="market_data", print_msg=f"{data_to_write['instrument_name'].unique()}")
        #print("Succesful write in db: ", write_in_db)


    def get_option_info(self, instrument_name:str, pit=int(dt.datetime.now(tz=dt.timezone.utc).timestamp()*1000)) -> dict:
        ccxt.Exchange.load_markets()
        ohlcv_data = self.fetch_ohlcv(instrument_name,"1m").iloc[-1,:]
        option_data = self.deribit.market(instrument_name)
        underlying_market = option_data["info"]["price_index"]
        underlying_price = float(self.deribit.publicGetGetIndexPrice(params={"index_name":underlying_market})["result"]["index_price"]) # type: ignore
        strike = float(option_data["strike"])
        expiration_timestamp = dt.datetime.fromtimestamp(int(option_data["expiry"])/1000, tz= dt.timezone.utc)
        option_type = option_data["info"]["option_type"]
        instrument_name = option_data["info"]["instrument_name"]
        implied_volatility = float(self.deribit.fetch_volatility_history(option_data["base"])[-1]["volatility"])/100


        return {"instrument_name": instrument_name,
                "strike": strike,
                "expiration_timestamp": expiration_timestamp,
                "implied_volatility": implied_volatility,
                "option_type": option_type,
                "underlying_market": underlying_market,
                "underlying_price":underlying_price,
                "open": ohlcv_data["open"],
                "high": ohlcv_data["high"],
                "low": ohlcv_data["low"],
                "close": ohlcv_data["close"],
                "volume": ohlcv_data["volume"],
                "timestamp": ohlcv_data["timestamp"],
                "maturity": (expiration_timestamp - ohlcv_data["timestamp"]).total_seconds()/3600
                }


    # Index from Binance
        
    def get_price_index(self, index_name, timestamp:int) -> float:
        if self.deribit.milliseconds() - timestamp < (60*60*1000):
            index_price = self.deribit.publicGetGetIndexPrice(params={"index_name": index_name})
        else:
            since = timestamp - 60 * 1000
            ohlcv = self.deribit.fetch_ohlcv(index_name, "1m", since=since, limit=1)
            index_price = sum(ohlcv[0][1:5])/len(ohlcv[0][1:5])

        return index_price
    

    def get_index_data(self, index_name:str, interval:str="1m", start_timestamp:dt.datetime=None):
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
        data = self.binance.fetch_ohlcv(
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


    def read_last_date_from_index(self, index_name:str, exchange:str, year:int = dt.datetime.now().year) -> dt.datetime:
        """
        Fetch last timestamp from the "assets" filtered by assets_name:
        and the first (if get_min_date = True) and last timestamp available in market_data

        SQL SNIPPET
        ------------------------------
        SELECT index_name, exchange, max(timestamp) as last_timestamp
        FROM index_data_{year}
        GROUP BY asset_name, exchange
        """

        # Build SQL snippets for execution
        
        sql = f"SELECT max(timestamp) as last_timestamp\
            FROM index_data_{year}\
            WHERE index_name = '{index_name}' AND exchange = '{exchange}'"
        # Define dfresult and dtypes
        #dtypes = {
        #    "asset_name": "str",
        #    "exchange": "str",
        #    "last_timestamp": "datetime64"
        #}
        #print(f"SQL to execute:\n {sql}")

        # initialize the cursor and execute the SQL sentence
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(sql)
                qresult = cursor.fetchall()
        except Exception as err:
            print(f"Exception raised:{err}")

        #print("Results in query",len(qresult))
        #dfresult = pd.DataFrame(qresult, columns=[desc[0] for desc in cursor.description]) #.astype(dtypes)
        #print(dfresult.info())

        return qresult[0][0]
    

    def check_index_data_year_complete(self, index_name:str, year:int, exchange:str="binance"):
        last_timestamp = self.read_last_date_from_index(index_name=index_name, exchange=exchange, year=year)
        if last_timestamp < dt.datetime(year=year, month=12, day=31, hour=0, minute=59):
            return last_timestamp
        else:
            print(f"{year} is complete, checking the next one!!")
            return self.check_index_data_year_complete(index_name=index_name,year=year+1, exchange=exchange)


    def write_index_data(self, index_name:str, interval:str, start_timestamp:dt.datetime, end_timestamp:dt.datetime=dt.datetime.utcnow(), exchange:str="binance"):
        """
        Retrive and write index data from Binance.
        Accepted intervals: ['1s', '1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1M']
        """

        print(f"Start collecting {index_name} since {start_timestamp} to {end_timestamp}")
        print()
        last_timestamp = self.check_index_data_year_complete(index_name=index_name, year=last_timestamp.year, exchange="binance")
        print(f"Last timestamp in DB is {last_timestamp}")


        # Check valid intervals
        valid_intervals = ['1s', '1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1M']
        if not interval in valid_intervals:
            print("Invalid interval")
            return

        # Initialize data collector for bulk writing in DB
        data_to_write = pd.DataFrame()

        # Refresh end timestamp to now
        end_timestamp = dt.datetime.utcnow()

        # Loop until end timestamp is reached
        while last_timestamp < end_timestamp:

            # retrive data from the last available data in the DB or from the startTimestamp defined
            index_data = self.get_index_data(
                index_name=index_name,
                interval=interval,
                start_timestamp=last_timestamp
                )

            if index_data.empty:
                print(f"Returned empty getting data from:\n\
                      {index_name} - {last_timestamp}")
                
            last_timestamp = index_data["timestamp"].max()

            # Append retrived data to bulk writer
            if data_to_write.empty:
                data_to_write = index_data
            else:
                data_to_write = pd.concat([data_to_write, index_data], ignore_index=True)

            # Write to the DB if the bulk container is full
            if len(data_to_write) >= 25000:

                # check if there are data from different years
                years = data_to_write["timestamp"].dt.year.unique().tolist()
                print(f"Data from these years {years}")
                
                # Iterate over the year values in data_to_write and write the data to DB by years
                for year in years:
                    self.write_df_to_db(data=data_to_write[data_to_write["timestamp"].dt.year == year], table_name=f"index_data_{year}", print_msg=f"{index_name}-{year}")
                
                # Empty collector
                data_to_write = pd.DataFrame()

            print(f"{index_name} last date: {last_timestamp}")
            print(f"data to write size: {data_to_write.shape}\n")

        # Write any remining data in the collector
        # check if there are data from different years
        years = data_to_write["timestamp"].dt.year.unique().tolist()
        print(f"Data from these years {years}")
        # Iterate over the year values in data_to_write and write the data to DB by years
        for year in years:
            self.write_df_to_db(data=data_to_write[data_to_write["timestamp"].dt.year == year], table_name=f"index_data_{year}",  print_msg=f"{index_name}-{year}")
        print(f"{index_name} last date: {last_timestamp}")
        print(f"data to write size: {data_to_write.shape}\n")
