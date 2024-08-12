from . import fetch_ohlcv
def get_price_index(deribit_obj, index_name, timestamp:int) -> float:
    if deribit_obj.milliseconds() - timestamp < (60*60*1000):
        index_price = deribit_obj.publicGetGetIndexPrice(params={"index_name": index_name})
    else:
        since = timestamp - 60 * 1000
        ohlcv = fetch_ohlcv(deribit_obj=deribit_obj, index_name=index_name, resolution= 1, since=since, limit=1)
        index_price = sum(ohlcv[0][1:5])/len(ohlcv[0][1:5])

    return index_price