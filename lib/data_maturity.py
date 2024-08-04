from .Agent import Collector
import json
from variables import *
import datetime as dt
import time
import pandas as pd

collector = Collector()

collector.init_db_conn(
    db_name=db_name,
    db_user=db_user,
    db_password=db_password
)


if __name__ == "__main__":
    
    instruments = collector.execute_sql(
        "SELECT instrument_name, expiration_timestamp FROM instruments"
    )

    print(instruments)
