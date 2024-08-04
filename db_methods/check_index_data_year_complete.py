import datetime as dt
import db_methods

def check_index_data_year_complete(self, index_name:str, year:int, exchange:str="binance"):
        last_timestamp = db_methods.read_last_date_from_index(index_name=index_name, exchange=exchange, year=year)
        if last_timestamp < dt.datetime(year=year, month=12, day=31, hour=0, minute=59):
            return last_timestamp
        else:
            print(f"{year} is complete, checking the next one!!")
            return check_index_data_year_complete(index_name=index_name,year=year+1, exchange=exchange)
