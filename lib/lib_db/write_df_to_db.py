import pandas as pd
from .execute_sql import execute_sql
from ..Agent import Collector
from .write_df_to_parquet import write_df_to_delta
import os
from sqlalchemy import create_engine

__all__ = ["write_df_to_db"]

def write_df_to_db(agent:Collector, data:pd.DataFrame, table_name:str, output_path:str=None, exchange:str=None, verbose=False) -> tuple:
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


    elif agent.db_type == "athena":
        
        # Define partitions cols for market_data
        if table_name == "market_data":
            partition_cols = ["p-exchange","p-price_index", "p-year", "p-instrument_type"]
            data["p-price_index"] = data['price_index']
            data["p-exchange"] = data["exchange"]
            data['p-year'] = data['timestamp'].dt.year
            data["p-instrument_type"] = data["instrument_type"]
        
        else:
            partition_cols = []

        storage_options = {
            'anon': False
        }

        write_df_to_delta(
            data=data,
            output_path=f"{output_path}/{table_name}",
            partition_cols=partition_cols,
            storage_options=storage_options
        )
        status = True
        writen_rows = len(data)
        writen_failed = 0
    
    # Return writing status
    return (status, writen_rows, writen_failed)
