import pandas as pd
from ..Agent import Collector
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
import time
import psycopg2

__all__ = ['execute_sql']

def execute_sql(sql:str, agent:Collector, values=None, verbose=False):
    """
	Execute a SQL query on a given database connection.\n

	**Parameters:**
		agent (object): A database connection object.
		sql (str): The SQL query to be executed.
		output (str): The output return type, either "list" or "df" (default is "list").

	**Returns:**
		list: A list of rows returned by the SQL query if it is a SELECT query.
		pd.DataFrame: A pandas DataFrame returned by the SQL query if output is "df".
		None: If the query is not a SELECT query.
	"""

    # Filter the query statement
    if sql.startswith("SELECT"):
        try:
            dfresult = pd.read_sql(sql=sql, con=agent.conn)
            return dfresult

        except Exception as err:
            print(f"Exception raised:{err}")

    # Not a SELECT query
    else:
        if agent.db_type == "postgres":
            if agent.session == None:
                Session = sessionmaker(bind=agent.conn)
                agent.session = Session()
            elif not agent.session.is_active:
                Session = sessionmaker(bind=agent.conn)
                agent.session = Session()
                
            # Check if transaction is in progress
            while agent.session.in_transaction():
                if verbose == True:
                    print("Transaction in progress", end="\r")
                time.sleep(0.1)

            try:
                agent.session.execute(text(sql), params = values)
                agent.session.commit()

            except Exception as err:
                    if verbose:
                        print(f"Exception raised:{err}")
                    
                    agent.session.rollback()
            
            else:
                if verbose:
                    print("Execute SQL succesful")
        
        # PENDING IMPLEMENTATiON OF UPDATING DATA IN ATHENA DB
        elif agent.db_type == "athena":
            pass
