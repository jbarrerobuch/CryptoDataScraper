from lib.lib_db import execute_sql
from lib.init_agent import init_agent
import os
import pandas as pd
from dotenv import load_dotenv


load_dotenv()

#conn = connect(
#    s3_staging_dir="s3://el-oraculo-de-pepe/crypto-data-athena/",
#    region_name="us-east-1"
#)
var = os.getenv("AWS_ACCESS_KEY")
agent = init_agent(db="athena", deribit=False, binance=False)

sql=f"SELECT * FROM market_data LIMIT 10"
df = execute_sql(sql=sql, db_conn=agent.conn)

print(df)