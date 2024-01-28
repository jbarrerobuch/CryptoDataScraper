import Agent as Agent
import json
from variables import *

collector = Agent.Collector()

collector.init_deribit(
    apiKey=deribit_apiKey,
    apisecret=deribit_apisecret,
    sandbox_mode=False
)
collector.init_db_conn(
    db_name=db_name,
    db_user=db_user,
    db_password=db_password
)


currencies = collector.get_currency_list()
print(currencies)

instruments = collector.get_instruments(currency_list=currencies)
instruments["is_complete"] = False
instruments["tick_size_steps"] = instruments["tick_size_steps"].apply(json.dumps)
print(instruments.head())
write_in_db = collector.write_df_to_db(instruments, "instruments")
print(write_in_db)
