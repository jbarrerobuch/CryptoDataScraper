import polars as pl
from .execute_sql import execute_sql
from ..Agent import Collector
from .write_df_to_delta import write_df_to_delta
from ..delete_df import delete_df
from sqlalchemy import create_engine

__all__ = ["write_df_to_db"]

def write_df_to_db(agent:Collector, data:pl.DataFrame, table_name:str, output_path:str=None, exchange:str=None, verbose=False) -> tuple:
    """
    Bulk write a dataframe to a table.
    Required:
    - Dataframe's column names match DB_table's columns names
    - Default index. It will be reset before writing
    """
    
    if agent.db_type == "postgres":
        writen_rows = 0
        writen_failed = 0
        # Reset dataframe's index to iterate over it
        data.reset_index(drop=True, inplace=True)
        # Convert values from dataframe to dictionary
        status = False
        values = data.to_dict(orient="index")

        if verbose == True:
            print("================")
            print(f"Writing {len(data)} data points.")

        
        # Gather column names and concatenate them comma separated
        col_names = data.columns.to_list()
        col_names_csv = ",".join(col_names)

        # Map column name in dataframe to same name column in the DB
        col_names_map = ",".join([f":{i}" for i in col_names])

        # Build the argument value to execute the query
        arg = f"INSERT INTO {table_name} ({col_names_csv}) VALUES ({col_names_map})"
        # Writing each row by iterating the value list
        for i in range(len(values)):

            try:
                execute_sql(sql=arg, agent=agent, values=values[i], verbose=verbose)
                status = True
                writen_rows += 1
            except Exception as e:
                if verbose == True:
                    print(f"Writing data failed... Rollback - Error:\n values {i} - {e}")
                writen_failed += 1
        
        if not agent.session == None:
            agent.session.close()


    elif agent.db_type == "delta":
        
        # Define partitions cols for market_data
        if table_name == "market_data":
            partition_cols = ["exchange","price_index", "date", "instrument_id"]
        
        else:
            partition_cols = []

        
        # Define storage options
        if output_path.startswith("s3"):
            storage_options = {
                'anon': False
            }
        
        else:
            storage_options = {}

        write_df_to_delta(
            data=data,
            output_path=output_path,
            table_name=table_name,
            partition_cols=partition_cols,
            storage_options=storage_options
        )

    status = True
    writen_rows = len(data)
    writen_failed = 0

    delete_df(data)

    # Return writing status
    return (status, writen_rows, writen_failed)
