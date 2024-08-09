from variables import *
import datetime as dt
import time
from lib import *
import memory_profiler as mp


def main(index_list:list, iteration_sleep:int=60*60*24) -> None:
    
    start = time.time()
    memory, agent = mp.memory_usage((init_agent, (), {}), retval=True, max_usage=True)
    end = time.time()
    agent.memory_usage[init_agent.__name__] = (memory, (end - start))

    # Init timer
    since_last_iteration = 0

    # Launch infinite Loop
    while True:

        # Update and print iteration start time
        agent.iteration_start = dt.datetime.now()
        print(f"Iteration start: {agent.iteration_start}")

        
        if since_last_iteration == 0 or (since_last_iteration < dt.timedelta(seconds=iteration_sleep) and since_last_iteration > dt.timedelta(days=1)):
            profile_function(
                func=db_methods.write_instruments_table_deribit,
                agent=agent,
                args=(),
                kwargs={
                    "deribit_obj": agent.deribit,
                    "db_conn": agent.conn,
                    "is_active": False
                }
            )
            profile_function(
                func=db_methods.write_instruments_table_deribit,
                agent=agent,
                args=(),
                kwargs={
                    "deribit_obj": agent.deribit,
                    "db_conn": agent.conn,
                    "is_active": True
                }
            )


        if since_last_iteration == 0 or (since_last_iteration < dt.timedelta(seconds=iteration_sleep) and since_last_iteration > dt.timedelta(hours=1)):

            # Update active status of instruments.
            profile_function(
                func=db_methods.write_instruments_status,
                agent=agent,
                args=(),
                kwargs={"db_conn": agent.conn}
            )

        # Gather and store data from expired instruments
        retval = profile_function(
            func=db_methods.write_marketdata_in_db,
            agent=agent,
            args=(),
            kwargs={
                "db_conn": agent.conn,
                "deribit_obj": agent.deribit,
                "is_active": False
            }
        )
        written_rows, failed_rows = retval
        agent.data_written.append(written_rows)
        agent.data_failed.append(failed_rows)

        # Gather and store data from active intruments
        retval = profile_function(
            func=db_methods.write_marketdata_in_db,
            agent=agent,
            args=(),
            kwargs={
                "db_conn": agent.conn,
                "deribit_obj": agent.deribit,
                "is_active": True
            }
        )
        written_rows, failed_rows = retval
        agent.data_written.append(written_rows)
        agent.data_failed.append(failed_rows)

        for i in index_list:
            
            # Get last timestamp from the index in db
            start_timestamp = profile_function(
                func=db_methods.read_last_date_from_index,
                agent=agent,
                args=(),
                kwargs={
                    "db_conn": agent.conn,
                    "index_name": i,
                    "exchange": "binance"
                }
            )

            profile_function(
                func=db_methods.write_index_data,
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

        time.sleep(iteration_sleep)
        since_last_iteration = dt.datetime.now() - agent.iteration_end


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
    main(index_list=index_list, iteration_sleep=(60*10))
