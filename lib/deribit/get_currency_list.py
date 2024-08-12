def get_currency_list(deribit_obj) -> list:
    currencies_raw = deribit_obj.publicGetGetCurrencies() # type: ignore
    currency_list = []
    for currency in currencies_raw["result"]:
        currency_list.append(currency["currency"])
    return currency_list