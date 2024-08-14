import ccxt

__all__ = ["get_currency_list"]

def get_currency_list(deribit_obj:ccxt.deribit) -> list:
    """
    Retrieves a list of available currencies from the Deribit exchange.

    Args:
        deribit_obj (ccxt.deribit): A Deribit API object.

    Returns:
        list: A list of currency names.
    """    
    currencies_raw = deribit_obj.publicGetGetCurrencies() # type: ignore
    currency_list = []
    for currency in currencies_raw["result"]:
        currency_list.append(currency["currency"])
    return currency_list