from .Agent import Collector
from .init_agent import init_agent
import datetime as dt
import time
import pandas as pd
from . import lib_db

def index_data_gap_finder(agent:Collector,index_name:str, year:int) -> list:
    """
    This function identifies gaps in index data for a given year and index name.

    It queries the database to retrieve the index data for the specified year and index name,
    and then uses pandas to calculate the time differences between consecutive data points.
    The function identifies gaps in the data where the time difference is greater than 1 minute.

    The function returns a list of dictionaries, where each dictionary represents a gap in the data.
    Each dictionary contains the start and end timestamps of the gap.

    Parameters:
        agent (Collector): The agent used to execute the database query.
        index_name (str): The name of the index for which to retrieve data.
        year (int): The year for which to retrieve data.

    Returns:
        list: A list of dictionaries, where each dictionary represents a gap in the data.
    """

    # Query to read DB
    query = f"SELECT * FROM index_data_{year} WHERE index_name = '{index_name}' ORDER BY timestamp ASC"

    data = lib_db.execute_sql(db_conn=agent.conn, query=query)

    df = pd.DataFrame().from_records(
        data=data,
        columns=[
            "data_id",
            "index_name",
            "exchange",
            "timestamp",
            "open",
            "high",
            "low",
            "close"
            ])

    t_diff = df["timestamp"].diff()
    diff_1min = t_diff <= pd.Timedelta(minutes=1)
    index_gaps = df.loc[diff_1min == False, "timestamp"]

    print(index_gaps)
    result = []

    # Identify gap from start of the year
    if not df.iloc[0]["timestamp"] == dt.datetime(year=year, month=1, day=1, hour=0, minute=0):
        result.append({"start": dt.datetime(year=year, month=1, day=1, hour=0, minute=0), "end": df.iloc[0]["timestamp"]})

    #print("--------Ranges-----------")
    for i, value in index_gaps.items():
        #print(i, value)
        missing_range = {}

        missing_range["start"] = df.iloc[i-1,3] + dt.timedelta(minutes=1),
        missing_range["end"] = df.iloc[i,3] - dt.timedelta(minutes=1)
        
        result.append(missing_range)

    #print(result)
    return result
