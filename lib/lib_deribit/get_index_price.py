from .fetch_candles import *
import ccxt

__all__ = ["get_price_index"]

def get_price_index(deribit_obj:ccxt.deribit, index_name, timestamp:int) -> float:
    """
    Retrieves the current index price from Deribit.

    Parameters:
    deribit_obj (object): A Deribit API object.
    index_name (str): The name of the index.
    timestamp (int): The timestamp to check against.

    Returns:
    float: The current index price.
    """
    if deribit_obj.milliseconds() - timestamp < (60*60*1000):
        index_price = deribit_obj.publicGetGetIndexPrice(params={"index_name": index_name})
    else:
        since = timestamp - 60 * 1000
        ohlcv = fetch_candles(deribit_obj=deribit_obj, index_name=index_name, resolution= 1, since=since, limit=1)
        index_price = sum(ohlcv[0][1:5])/len(ohlcv[0][1:5])

    return index_price