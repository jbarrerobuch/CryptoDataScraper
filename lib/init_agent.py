from variables import *
from .Agent import Collector

def init_agent(db:bool=True, binance:bool=True, deribit:bool=True) -> Collector:

    # Init the Collector class
    agent = Collector()
    print()

    # Init Deribit connection
    if deribit:
        agent.init_deribit(
            apiKey=deribit_apiKey,
            apisecret=deribit_apisecret,
            sandbox_mode=False
        )

    # Init Binance connection
    if binance:
        agent.init_binance(
            apiKey=binance_apiKey,
            apisecret=binance_apisecret,
            sandbox_mode=False
        )


    # Init Postgress BD connection
    if db:
        agent.init_db_conn(
            db_name=db_name,
            db_user=db_user,
            db_password=db_password
        )

    return agent