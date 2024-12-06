import os 
from dotenv import load_dotenv
import pymssql
import pandas as pd

def connect_to_db():
    load_dotenv(override=True)
    hostname=os.getenv("HOSTNAME")
    database=os.getenv("DATABASE")
    username=os.getenv("USERNAME")
    password=os.getenv("PASSWORD")
    
    print(hostname,database,username,password)
    try :
        conn=pymssql.connect(hostname, username, password, database)
    except:
        raise Exception("Connection failed!")
    print("Connection successful!")
    return conn

def read_sql_query(sql_query):
    """
    Executes the SQL query on the SQLite3 database and retrieves the results.
    Args:
        sql_query (str): The SQL query to execute.
    Returns:
        list: The rows retrieved from the database.
    """
    conn=connect_to_db()
    cursor = conn.cursor()
    cursor.execute(sql_query)
    columns=[col[0] for col in cursor.description]
    rows = cursor.fetchall()
    res_df=pd.DataFrame(rows,columns=columns)
    conn.close()
    
    return res_df
