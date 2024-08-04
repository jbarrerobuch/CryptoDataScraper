import pandas as pd

def read_last_date_from_instruments(db_conn, is_active:bool=True, is_complete:bool=False, get_last_data_timestamp = False) -> pd.DataFrame:
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
        with db_conn.cursor() as cursor:
            cursor.execute(sql)
            qresult = cursor.fetchall()
        db_conn.commit()
    except Exception as err:
        print(f"Exception raised:{err}")
        db_conn.rollback()

    #print("Results in query",len(qresult))
    dfresult = pd.DataFrame(qresult, columns=[desc[0] for desc in cursor.description]).astype(dtypes)
    #print(dfresult.info())

    return dfresult