import datetime as dt

__all__ = ["read_last_date_from_index"]

def read_last_date_from_index(db_conn, index_name:str, exchange:str, year:int = dt.datetime.now(dt.timezone.utc).year, timezone:dt.timezone = dt.timezone.utc) -> dt.datetime:
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
        # use excuted_sql ??
        try:
            with db_conn.cursor() as cursor:
                cursor.execute(sql)
                qresult = cursor.fetchall()
        except Exception as err:
            print(f"Exception raised:{err}")

        last_date = qresult[0][0]
        last_date = last_date.replace(tzinfo=timezone)
        return last_date