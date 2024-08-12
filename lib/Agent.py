import ccxt
import datetime as dt
from pytz import timezone, all_timezones
import psycopg2
import os
import json

class Collector:

    def __init__(self) -> None:
        self.deribit = None
        self.binance = None
        self.conn = None
        self.cur = None
        self.data_written = []
        self.data_failed = []
        self.memory_usage = {}
        self.iterations = 0
        self.session_start = dt.datetime.now()
        self.iteration_start = dt.datetime.now()
        self.iteration_end = dt.datetime.now()
        self.iteration_duration = self.iteration_end - self.iteration_start
        self.running_time = dt.timedelta(seconds=0)
        self.log_filename = f"log/{self.session_start.strftime('%Y%m%d-%H%M')}.csv"


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
    
    def write_log(self) -> None:
        if not os.path.exists(self.log_filename):
            with open(self.log_filename, 'w') as f:
                f.write("log_timestamp| session_start| running_time| iteration_start| iteration_end| iteration_duration| data_written| data_failed| memory_usage\n")
                f.write(f"{dt.datetime.now()}|{self.session_start}|{self.running_time}|{self.iteration_start}|{self.iteration_end}|{self.iteration_end - self.iteration_start}|{self.data_written[-1]}|{self.data_failed[-1]}|{json.dumps(self.memory_usage)}\n")
        else:
            with open(self.log_filename, 'a') as f:
                f.write(f"{dt.datetime.now()}|{self.session_start}|{self.running_time}|{self.iteration_start}|{self.iteration_end}|{self.iteration_end - self.iteration_start}|{self.data_written[-1]}|{self.data_failed[-1]}|{json.dumps(self.memory_usage)}\n")
