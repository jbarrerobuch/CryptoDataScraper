from dotenv import load_dotenv
import datetime as dt
import os
import time
import lib
import memory_profiler as mp

def main(index_list:list, iteration_sleep:int=60*60*24, db_type:str=None, output_path:str=None) -> None:
        
    if db_type == "postgres":
        kwargs = {"db": "postgres"}
    elif db_type == "athena":
        kwargs = {"db": "athena"}
    else:
        kwargs = {"db": False}

    start = time.time()
    memory, agent = mp.memory_usage((lib.init_agent, (), kwargs), retval=True, max_usage=True)
    end = time.time()
    agent.memory_usage[lib.init_agent.__name__] = (memory, (end - start))

    # Init timer
    since_last_iteration = 0

    # Launch infinite Loop
    while True:

        # Update and print iteration start time
        agent.iteration_start = dt.datetime.now()
        print(f"Iteration start: {agent.iteration_start}")

        
        if since_last_iteration == 0 or (since_last_iteration < dt.timedelta(seconds=iteration_sleep) and since_last_iteration > dt.timedelta(days=1)):
            
            # Define Kwargs
            kwargs = {
                "agent": agent,
                "output_path": output_path
                }
            
            # define is_actve = False for write_instruments_table_deribit
            kwargs["is_active"] = False
            lib.profile_function(
                func=lib.write_instruments_table_deribit,
                agent=agent,
                args=(),
                kwargs=kwargs
            )

            # define is_actve = True for write_instruments_table_deribit
            kwargs["is_active"] = True
            lib.profile_function(
                func=lib.write_instruments_table_deribit,
                agent=agent,
                args=(),
                kwargs=kwargs
            )


        if agent.db_type == "postgres" and (since_last_iteration == 0 or (since_last_iteration < dt.timedelta(seconds=iteration_sleep) and since_last_iteration > dt.timedelta(hours=1))):

            # Update active status of instruments.
            lib.profile_function(
                func=lib.write_instruments_status,
                agent=agent,
                args=(),
                kwargs={
                    "db_conn": agent.conn
                }
            )

        # Gather and store data from expired instruments
        lib.profile_function(
            func=lib.write_marketdata,
            agent=agent,
            args=(),
            kwargs={
                "agent": agent,
                "is_active": False,
                "output_path": output_path
            }
        )

        # Gather and store data from active intruments
        lib.profile_function(
            func=lib.write_marketdata,
            agent=agent,
            args=(),
            kwargs={
                "agent": agent,
                "is_active": True,
                "output_path": output_path
            }
        )

        for i in index_list:
            
            # Get last timestamp from the index in db
            start_timestamp = lib.profile_function(
                func=lib.read_last_date_from_index,
                agent=agent,
                args=(),
                kwargs={
                    "db_conn": agent.conn,
                    "index_name": i,
                    "exchange": "binance"
                }
            )

            lib.profile_function(
                func=lib.write_index_data,
                agent=agent,
                args=(),
                kwargs={
                    "db_conn": agent.conn,
                    "binance_obj": agent.binance,
                    "index_name": i,
                    "interval": "1m",
                    "start_timestamp": start_timestamp
                }
            )

        # Calculate stats
        agent.iteration_end = dt.datetime.now()
        agent.iteration_duration = agent.iteration_end - agent.iteration_start
        iteration_sleep = agent.iteration_duration.seconds * 1.1 # Modify duration to +10% of the duration
        agent.running_time += agent.iteration_duration
        agent.iterations += 1

        total_memory = sum([i[0] for i in list(agent.memory_usage.values())])

        # Print memory usage
        print("======= [MEMORY STATS] =======")
        print(agent.memory_usage)
        print("=============================")
        print()

        # Print the stats
        print("======= [SESSION STATS] =======")
        print("Session start: ", agent.session_start)
        print("Session duration: ", dt.datetime.now() - agent.session_start)
        print("Duration of iteration", agent.iteration_duration)
        #print(f"Datapoints\nWriten: {agent.data_written[-1]}\nFailed: {agent.data_failed[-1]}")
        print(f"Running time: {agent.running_time}")
        print(f"Iterarions: {agent.iterations}")
        print(f"Running avg per iteration: {round((agent.running_time.seconds/60)/agent.iterations,2)} minutes")
        print(f"Writing speed: {sum(agent.data_written)/(agent.running_time.seconds/60)}")
        print(f"Memory usage: {total_memory}")
        print("=============================")
        print()
        print(dt.datetime.now(), f"sleeping for {iteration_sleep/60} minutes\n")
        print(f"Next iteration starts at {dt.datetime.now() + dt.timedelta(seconds=iteration_sleep)}")
        print()

        # Write log
        agent.write_log()
        agent.memory_usage = {}

        time.sleep(iteration_sleep)
        since_last_iteration = dt.datetime.now() - agent.iteration_end


if __name__ == "__main__":

    # Load .env file
    env_loaded = load_dotenv(dotenv_path=".env")
    if not env_loaded:
        exit(1)
    
    else:
        print("Loaded .env file")

    env_vars = os.getenv("AWS_ACCESS_KEY")

    partition_cols = []

    # to be added in agents init
    storage_options = {'anon': False}
    path = "s3://scraped-cryptodata"
    bucket_name = "scraped-cryptodata"
    
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

    main(
        index_list=index_list,
        iteration_sleep=(60*10),
        db_type="postgres",
        #output_path="s3://scraped-cryptodata"
    )
