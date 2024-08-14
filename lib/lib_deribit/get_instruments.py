import pandas as pd

__all__ = ["get_instruments"]

def get_instruments(deribit_obj, currency_list:list, kind="option", expired = False) -> pd.DataFrame:
    dtypes = {
            "tick_size_steps": "str",
            "quote_currency": "str",
            "min_trade_amount": "float",
            "expiration_timestamp": "float",
            "counter_currency": "str",
            "settlement_currency": "str",
            "block_trade_tick_size": "float",
            "block_trade_min_trade_amount": "float",
            "block_trade_commission": "float",
            "option_type": "str",
            "settlement_period": "str",
            "creation_timestamp": "float",
            "contract_size": "float",
            "base_currency": "str",
            "instrument_id": "str",
            "instrument_type": "str",
            "taker_commission": "float",
            "maker_commission": "float",
            "tick_size": "float",
            "strike": "float",
            "is_active": "bool",
            "instrument_name": "str",
            "kind": "str",
            "rfq": "bool",
            "price_index": "str",
            "is_complete": "bool"
    }
    instruments_df = pd.DataFrame()
    for currency in currency_list:
        instruments_raw = deribit_obj.publicGetGetInstruments(
            params={
                "currency": currency, # type: ignore
                "kind": kind,
                "expired": expired
            }
        )
        data = pd.DataFrame.from_records(instruments_raw["result"])
        if not data.empty:
            data["is_complete"] = False
            if instruments_df.empty:
                instruments_df = data
            else:
                instruments_df = pd.concat([instruments_df, data], ignore_index=True)
    

    instruments_df = instruments_df.astype(dtypes)    
    instruments_df['expiration_timestamp'] = pd.to_datetime(instruments_df['expiration_timestamp'], origin="unix", unit="ms", utc= True)
    instruments_df['creation_timestamp'] = pd.to_datetime(instruments_df['creation_timestamp'], origin="unix", unit="ms", utc= True)
    
    
    return instruments_df