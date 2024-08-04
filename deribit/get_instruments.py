import pandas as pd

def get_instruments(deribit_obj, currency_list:list, kind="option", expired = False) -> pd.DataFrame:
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
            "is_complete": "bool"
    }
    instruments_df = pd.DataFrame(columns = dtypes.keys()).astype(dtypes)
    for currency in currency_list:
        instruments_raw = deribit_obj.publicGetGetInstruments(
            params={
                "currency": currency, # type: ignore
                "kind": kind,
                "expired": expired
            }
        )
        data = pd.DataFrame.from_records(instruments_raw["result"])
        instruments_df = pd.concat([instruments_df, data])
        instruments_df.reset_index(drop=True, inplace=True)


    instruments_df['expiration_timestamp'] = pd.to_datetime(instruments_df['expiration_timestamp'], unit="ms", utc= True)
    instruments_df['creation_timestamp'] = pd.to_datetime(instruments_df['creation_timestamp'], unit="ms", utc= True)
    return instruments_df