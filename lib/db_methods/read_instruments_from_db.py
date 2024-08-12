import pandas as pd

def read_instruments_from_db(db_conn, is_active:bool=True, is_complete:bool=False) -> pd.DataFrame:

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
        with db_conn.cursor() as cursor:
            cursor.execute(sql)
            qresult = cursor.fetchall()
        db_conn.commit()
    except Exception as err:
        print(f"Exception raised:{err}")
        db_conn.rollback()

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