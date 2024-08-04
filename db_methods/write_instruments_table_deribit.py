import db_methods.write_df_to_db
import deribit
import json

def write_instruments_table_deribit(deribiti_obj, is_active= True):
    currencies = deribit.get_currency_list()
    print(f"Updating instruments with active status {is_active}")

    instruments = deribit.get_instruments(currency_list= currencies, expired= not is_active)
    instruments["is_complete"] = False
    instruments["tick_size_steps"] = instruments["tick_size_steps"].apply(json.dumps)
    #print(instruments.head())
    db_methods.write_df_to_db(data=instruments, table_name="instruments", verbose="False")
    #print(write_in_db)