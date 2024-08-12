from psycopg2 import connect
import pandas as pd

def execute_sql(db_conn:connect,query:str, output:str="list"):
    """
	Execute a SQL query on a given database connection.

	**Parameters:**
		db_conn (object): A database connection object.
		query (str): The SQL query to be executed.
		output (str): The output return type, either "list" or "df" (default is "list").

	**Returns:**
		list: A list of rows returned by the SQL query if it is a SELECT query.
		pd.DataFrame: A pandas DataFrame returned by the SQL query if output is "df".
		None: If the query is not a SELECT query.
	"""

    #print(f"SQL to execute:\n{sql}")
    
    try:
        with db_conn.cursor() as cursor:
            cursor.execute(query)

            # Filter the query statement
            if query.startswith("SELECT"):
                qresult = cursor.fetchall()

                # Output return type List or Dataframe
                if output == "list":
                    return qresult
                elif output == "df":
                    column_names = [desc[0] for desc in cursor.description]
                    dfresult = pd.DataFrame.from_records(
                        data=qresult,
                        columns=column_names
                    )
                    return dfresult

            else:
                db_conn.commit()
                
        print("Execute SQL succesful")
    
    except Exception as err:
        print(f"Exception raised:{err}")
        db_conn.rollback()