import pandas as pd
from ..Agent import Collector

__all__ = ['execute_sql']

def execute_sql(sql:str, agent:Collector, values=None, verbose=False):
    """
	Execute a SQL query on a given database connection.\n

	**Parameters:**
		db_conn (object): A database connection object.
		query (str): The SQL query to be executed.
		output (str): The output return type, either "list" or "df" (default is "list").

	**Returns:**
		list: A list of rows returned by the SQL query if it is a SELECT query.
		pd.DataFrame: A pandas DataFrame returned by the SQL query if output is "df".
		None: If the query is not a SELECT query.
	"""
    
    try:
        # Filter the query statement
        if sql.startswith("SELECT"):
            dfresult = pd.read_sql(sql=sql, con=agent.conn)
            return dfresult

        else:
            if agent.db_type == "postgres":
                with db_conn.cursor() as cursor:
                    cursor.execute(sql, values)
            
                db_conn.commit()
            
            # PENDING IMPLEMENTATiON OF UPDATING DATA IN ATHENA DB
            elif agent.db_type == "athena":
                pass
                
    except Exception as err:
        if verbose:
            print(f"Exception raised:{err}")
        db_conn.rollback()
    
    else:
        if verbose:
            print("Execute SQL succesful")
