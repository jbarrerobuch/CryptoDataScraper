import pandas as pd

def write_df_to_db(db_conn, data:pd.DataFrame, table_name:str, print_msg:str=None, verbose:str="False") -> bool:
    """
    Bulk write a dataframe to a table.
    Required:
    - Dataframe's column names match DB_table's columns names
    - Default index. It will be reset before writing
    """
    # Reset dataframe's index to iterate over it
    data.reset_index(drop=True, inplace=True)
    # Convert values from dataframe to dictionary
    status = False
    values = data.to_dict(orient="index")

    if print_msg:
        print("================")
        print(f"Writing {len(data)} data points.")
        print(print_msg)
    else:
        print("================")
        print(f"Writing {len(data)} data points to DB...")
    
    # Gather column names and concatenate them comma separated
    col_names = data.columns.to_list()
    col_names_csv = ",".join(col_names)

    # Map column name in dataframe to same name column in the DB
    col_names_map = ",".join([f"%({i})s" for i in col_names])

    # Build the argument value to execute the query
    arg = f"INSERT INTO {table_name} ({col_names_csv}) VALUES ({col_names_map})"
    writen_rows = 0
    writen_failed = 0
    # Writing each row by iterating the value list
    with db_conn.cursor() as cursor:
        status = False
        for i in range(len(values)):
            #print(f"values{i}")
            #print(values[i])
            #print()
            try:
                cursor.execute(arg, values[i])
                db_conn.commit()
                status = True
                writen_rows += 1
            except Exception as e:
                if verbose == "True":
                    print(f"Writing data failed... Rollback - Error:\n values{i} - {e}")
                db_conn.rollback()
                writen_failed += 1
    
    print("================")
    print(f"DB Writing ended\nWriten: {writen_rows}\nFailed: {writen_failed}\nTotal: {writen_rows+writen_failed}")
    print("================")
    print()

    # Return writing status
    return (status, writen_rows, writen_failed)
