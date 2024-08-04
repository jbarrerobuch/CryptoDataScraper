def execute_sql(db_conn,query:str):
        #print(f"SQL to execute:\n{sql}")
        try:
            with db_conn.cursor() as cursor:
                cursor.execute(query)
                if query.startswith("SELECT"):
                    return cursor.fetchall()
                else:
                    #qresult = cursor.rowcount
                    db_conn.commit()
            print("Execute SQL succesful")
        except Exception as err:
            print(f"Exception raised:{err}")
            db_conn.rollback()