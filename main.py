import lib.Agent as Agent
from variables import *
import datetime as dt
import time
from lib import *

def main(index_list:list, iteration_sleep:int=60*60*24) -> None:
    # Init the Collector class
    agent = Agent.Collector()

    # Init Deribit connection
    agent.init_deribit(
        apiKey=deribit_apiKey,
        apisecret=deribit_apisecret,
        sandbox_mode=False
    )

    # Init Binance connection
    agent.init_binance(
        apiKey=binance_apiKey,
        apisecret=binance_apisecret,
        sandbox_mode=False
    )

    # Init Postgress BD connection
    agent.init_db_conn(
        db_name=db_name,
        db_user=db_user,
        db_password=db_password
    )
    
    # Set up timestamp for stats
    session_start = dt.datetime.now()

    # Launch infinite Loop
    while True:

        iteration_start = dt.datetime.now()
        
        
        db_methods.write_instruments_table_deribit(
            deribiti_obj=agent.deribit,
            db_conn=agent.conn,
            is_active=False)
        db_methods.write_instruments_table_deribit(
            deribiti_obj=agent.deribit,
            db_conn=agent.conn,
            is_active=True)

        # Update active status of instruments.
        db_methods.write_instruments_status(db_conn=agent.conn)

        # Gather and store data from expired instruments
        written_rows, failed_rows = db_methods.write_marketdata_in_db(db_conn=agent.conn, is_active=False)
        agent.stats["datapoints"]["writen"] += written_rows
        agent.stats["datapoints"]["failed"] += failed_rows

        # Gather and store data from active intruments
        written_rows, failed_rows = db_methods.write_marketdata_in_db(db_conn=agent.conn, is_active=True)
        agent.stats["datapoints"]["writen"] += written_rows
        agent.stats["datapoints"]["failed"] += failed_rows

        for i in index_list:
            db_methods.write_index_data(
                db_conn=agent.conn,
                index_name=i,
                interval="1m",
                start_timestamp=db_methods.read_last_date_from_index(
                    db_conn=agent.conn,
                    index_name=i,
                    exchange="binance"
                    )
            )

        # Calculate stats
        iteration_end = dt.datetime.now()
        iteration_duration = iteration_end - iteration_start
        agent.stats["running_time"] += iteration_duration
        agent.stats["iterations"] += 1

        # Print the stats
        print("duration of iteration", iteration_duration)
        print("======= [SESSION STATS] =======")
        print("Session start: ", session_start)
        print("Session duration: ", dt.datetime.now() - session_start)
        print(f"Datapoints\nWriten: {agent.stats['datapoints']['writen']}\nFailed: {agent.stats['datapoints']['failed']}")
        print(f"Running time: {agent.stats['running_time']}")
        print(f"iterarions: {agent.stats['iterations']}")
        print(f"running avg per iteration: {round((agent.stats['running_time'].seconds/60)/agent.stats['iterations'],2)} minutes")
        print(f"Writing speed: {agent.stats['datapoints']['writen']/(agent.stats['running_time'].seconds/60)}")
        print("=============================")
        print()
        print(dt.datetime.now(), "sleeping for 24H\n")
        print(f"Next iteration starts at {dt.datetime.now() + dt.timedelta(hours=24)}")
        print()
        time.sleep(60*60*24)

if __name__ == "__main__":
    
    # Index list to retrieve
    index_list = [
        "ETHUSDT",
        "XRPUSDT",
        "LTCUSDT",
        "ADAUSDT",
        "DOGEUSDT",
        "SOLUSDT",
        "MATICUSDT",
        "BTCUSDT"
        ]
    main(index_list=index_list)