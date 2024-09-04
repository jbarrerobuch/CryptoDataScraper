from .Agent import *
import os

__all__ = ["init_agent"]

def init_agent(db:str=False, binance:bool=True, deribit:bool=True, sandbox:bool=False ) -> Collector:
    """
	Initialize a Collector agent with connections to Deribit, Binance, and a database.

	Parameters:
		db (str): The type of database to connect to (either "postgres" or "athena") or False.
		binance (bool): Whether to initialize a Binance connection (default is True).
		deribit (bool): Whether to initialize a Deribit connection (default is True).
		sandbox (bool): Whether to use sandbox mode for the connections (default is False).

	Returns:
		Collector: The initialized Collector agent.
	"""
    # Init the Collector class
    agent = Collector()
    print()

    # Init Deribit connection
    if deribit:
        agent.init_deribit(
            apiKey=os.getenv("DERIBIT_APIKEY"),
            apisecret=os.getenv("DERIBIT_APISECRET"),
            sandbox_mode=sandbox
        )

    # Init Binance connection
    if binance:
        agent.init_binance(
            apiKey=os.getenv("BINANCE_APIKEY"),
            apisecret=os.getenv("BINANCE_APISECRET"),
            sandbox_mode=sandbox
        )


    # Init DB connection Postgres or Athena
    if db == "postgres":
        agent.init_pg_conn()

    elif db == "athena":
        agent.init_athena_conn()
    elif db == "delta":
        agent.db_type = "delta"

    return agent