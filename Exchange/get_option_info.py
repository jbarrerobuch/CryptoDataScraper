import db_methods
import deribit
import datetime as dt

def get_option_info(deribit_obj, instrument_name:str, pit=int(dt.datetime.now(tz=dt.timezone.utc).timestamp()*1000)) -> dict:
        
        deribit_obj.load_markets() # changed cctx.Exchange by deribit object
        
        # Fetch candle data of instrument
        ohlcv_data = deribit.fetch_ohlcv(instrument_name,"1m").iloc[-1,:]

        # Fetch Instrument market information
        option_data = deribit_obj.market(instrument_name)

        # fetch underlying marjket of option instrument
        underlying_market = option_data["info"]["price_index"]
        underlying_price = float(deribit_obj.publicGetGetIndexPrice(params={"index_name":underlying_market})["result"]["index_price"]) # type: ignore
        strike = float(option_data["strike"])
        expiration_timestamp = dt.datetime.fromtimestamp(int(option_data["expiry"])/1000, tz= dt.timezone.utc)
        option_type = option_data["info"]["option_type"]
        instrument_name = option_data["info"]["instrument_name"]
        implied_volatility = float(deribit_obj.fetch_volatility_history(option_data["base"])[-1]["volatility"])/100

        return {"instrument_name": instrument_name,
                "strike": strike,
                "expiration_timestamp": expiration_timestamp,
                "implied_volatility": implied_volatility,
                "option_type": option_type,
                "underlying_market": underlying_market,
                "underlying_price":underlying_price,
                "open": ohlcv_data["open"],
                "high": ohlcv_data["high"],
                "low": ohlcv_data["low"],
                "close": ohlcv_data["close"],
                "volume": ohlcv_data["volume"],
                "timestamp": ohlcv_data["timestamp"],
                "maturity": (expiration_timestamp - ohlcv_data["timestamp"]).total_seconds()/3600
                }