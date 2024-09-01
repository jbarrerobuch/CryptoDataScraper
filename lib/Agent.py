import ccxt
import datetime as dt
from sqlalchemy import create_engine
import os
import json
import pandas as pd

__all__ = ["Collector"]
class Collector:

    def __init__(self) -> None:
        self.deribit = None
        self.binance = None
        self.conn = None
        self.session = None
        self.data_written = 0
        self.data_failed = 0
        self.memory_usage = {}
        self.iterations = 0
        self.session_start = dt.datetime.now()
        self.iteration_start = dt.datetime.now()
        self.iteration_end = dt.datetime.now()
        self.iteration_duration = self.iteration_end - self.iteration_start
        self.running_time = dt.timedelta(seconds=0)
        self.log_filename = f"log/{self.session_start.strftime('%Y%m%d-%H%M')}.csv"
        self.db_type = None
        self.verbose = False
        self.output_path = None


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


    def init_pg_conn(self, db_name:str=os.getenv("PG_DB_NAME"), db_user:str=os.getenv("PG_DB_USER"), db_password:str=os.getenv("PG_DB_PASSWORD")):
        self.conn = create_engine(f"postgresql://{db_user}:{db_password}@localhost:5432/{db_name}")

        self.db_type = "postgres"
        print("Postgres initialized")

    
    def init_athena_conn(self, s3_staging_dir:str=os.getenv("AWS_S3_STAGING_DIR"), schema_name:str=os.getenv("AWS_SCHEMA_NAME"), region_name:str=os.getenv("AWS_DEFAULT_REGION")):
        self.conn = create_engine(
        f'awsathena+rest://{os.getenv("AWS_ACCESS_KEY_ID")}:{os.getenv("AWS_SECRET_ACCESS_KEY")}@athena.{region_name}.amazonaws.com:443/?s3_staging_dir={s3_staging_dir}&schema_name={schema_name}'
        )

        try:
            pd.read_sql_query(sql="SELECT * FROM market_data LIMIT 1", con=self.conn)
        except Exception as e:
            print(f"Athena connection failed: {e}")
        else:
            self.db_type = "athena"
            print("Athena initialized")

    
    def write_log(self) -> None:
        if not os.path.exists(self.log_filename):
            with open(self.log_filename, 'w') as f:
                f.write("log_timestamp| session_start| running_time| iteration_start| iteration_end| iteration_duration| data_written| data_failed| memory_usage\n")
                f.write(f"{dt.datetime.now()}|{self.session_start}|{self.running_time}|{self.iteration_start}|{self.iteration_end}|{self.iteration_end - self.iteration_start}|{self.data_written}|{self.data_failed}|{json.dumps(self.memory_usage)}\n")
        else:
            with open(self.log_filename, 'a') as f:
                f.write(f"{dt.datetime.now()}|{self.session_start}|{self.running_time}|{self.iteration_start}|{self.iteration_end}|{self.iteration_end - self.iteration_start}|{self.data_written}|{self.data_failed}|{json.dumps(self.memory_usage)}\n")
