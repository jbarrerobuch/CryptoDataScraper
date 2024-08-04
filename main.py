import Agent as Agent
from variables import *
import datetime as dt
import time

import db_methods
import deribit

if __name__ == "__main__":

    # Init the Collector class
    collector = Agent.Collector()

    # Init Deribit connection
    collector.init_deribit(
        apiKey=deribit_apiKey,
        apisecret=deribit_apisecret,
        sandbox_mode=False
    )

    # Init Binance connection
    collector.init_binance(
        apiKey=binance_apiKey,
        apisecret=binance_apisecret,
        sandbox_mode=False
    )

    # Init Postgress BD connection
    collector.init_db_conn(
        db_name=db_name,
        db_user=db_user,
        db_password=db_password
    )

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
    
    # Set up timestamp for stats
    session_start = dt.datetime.now()

    # Launch infinite Loop
    while True:

        iter_start = dt.datetime.now()
        
        
        db_methods.write_instruments_table_deribit(collector.deribit, is_active=False)
        db_methods.write_instruments_table_deribit(collector.deribit)

        db_methods.write_instruments_status()
        collector.write_marketdata_in_db(is_active=False)
        collector.write_marketdata_in_db(is_active=True)

        for i in index_list:
            collector.write_index_data(
                index_name=i,
                interval="1m",
                start_timestamp=collector.read_last_date_from_index(index_name=i, exchange="binance")
            )


        iter_end = dt.datetime.now()
        iter_dur = iter_end - iter_start
        collector.stats["running_time"] += iter_dur
        collector.stats["iterations"] += 1
        print("duration of iteration", iter_dur)
        print("======= [SESSION STATS] =======")
        print("Session start: ", session_start)
        print("Session duration: ", dt.datetime.now() - session_start)
        print(f"Datapoints\nWriten: {collector.stats['datapoints']['writen']}\nFailed: {collector.stats['datapoints']['failed']}")
        print(f"Running time: {collector.stats['running_time']}")
        print(f"iterarions: {collector.stats['iterations']}")
        print(f"running avg per iteration: {round((collector.stats['running_time'].seconds/60)/collector.stats['iterations'],2)} minutes")
        print(f"Writing speed: {collector.stats['datapoints']['writen']/(collector.stats['running_time'].seconds/60)}")
        print("=============================")
        print()
        print(dt.datetime.now(), "sleeping for 24H\n")
        print(f"Next iteration starts at {dt.datetime.now() + dt.timedelta(hours=24)}")
        print()
        time.sleep(60*60*24)
