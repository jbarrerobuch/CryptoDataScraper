import datetime as dt
from .read_last_date_from_index import read_last_date_from_index

__all__ = ["check_index_data_year_complete"]

def check_index_data_year_complete(db_conn, index_name:str, year:int, exchange:str="binance"):
        last_timestamp = read_last_date_from_index(db_conn=db_conn, index_name=index_name, exchange=exchange, year=year)
        if last_timestamp < dt.datetime(year=year, month=12, day=31, hour=23, minute=59).replace(tzinfo=dt.timezone.utc):
            return last_timestamp
        else:
            print(f"{year} is complete, checking the next one!!")
            return check_index_data_year_complete(db_conn=db_conn, index_name=index_name,year=year+1, exchange=exchange)
