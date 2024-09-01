import polars as pl
import json

__all__ = ["get_instruments"]

def get_instruments(deribit_obj, currency_list: list, kind="option", expired=False) -> pl.DataFrame:
    dtypes = {
        "tick_size_steps": pl.Utf8,
        "quote_currency": pl.Utf8,
        "min_trade_amount": pl.Float64,
        "expiration_timestamp": pl.Float64,
        "counter_currency": pl.Utf8,
        "settlement_currency": pl.Utf8,
        "block_trade_tick_size": pl.Float64,
        "block_trade_min_trade_amount": pl.Float64,
        "block_trade_commission": pl.Float64,
        "option_type": pl.Utf8,
        "settlement_period": pl.Utf8,
        "creation_timestamp": pl.Float64,
        "contract_size": pl.Float64,
        "base_currency": pl.Utf8,
        "instrument_id": pl.Utf8,
        "instrument_type": pl.Utf8,
        "taker_commission": pl.Float64,
        "maker_commission": pl.Float64,
        "tick_size": pl.Float64,
        "strike": pl.Float64,
        "is_active": pl.Boolean,
        "instrument_name": pl.Utf8,
        "kind": pl.Utf8,
        "rfq": pl.Boolean,
        "price_index": pl.Utf8,
        "is_complete": pl.Boolean
    }

    Empty_tick_size_steps = {"above_price": "", "tick_size": ""}

    instruments_df = pl.DataFrame()

    for currency in currency_list:
        instruments_raw = deribit_obj.publicGetGetInstruments(
            params={
                "currency": currency,  # type: ignore
                "kind": kind,
                "expired": expired
            }
        )

        data = pl.DataFrame(instruments_raw["result"])

        if data.shape[0] > 0:
            data = data.with_columns(
                [
                    pl.lit(False).alias("is_complete"),
                    pl.col("tick_size_steps").fill_null(json.dumps(Empty_tick_size_steps)).alias("tick_size_steps") # Fill null values in the 'tick_size_steps' column with a default value
                ]
            )

            if instruments_df.shape[0] == 0:
                instruments_df = data
            else:
                instruments_df = pl.concat([instruments_df, data])
    
    instruments_df = instruments_df.with_columns_seq([
        pl.col(col).cast(dtype) for col, dtype in dtypes.items()
    ])
    
    instruments_df = instruments_df.with_columns([
        pl.col("expiration_timestamp").cast(pl.Datetime("ms")),
        pl.col("creation_timestamp").cast(pl.Datetime("ms"))
    ])

    return instruments_df