import pandas as pd 
from dotenv import load_dotenv
import pymssql
import os
from pathlib import Path
import requests
from datetime import datetime,timedelta 
import argparse
import math
import numpy as np 
from datetime import date

def connect_to_db():
    load_dotenv()
    hostname=os.getenv("HOSTNAME")
    database=os.getenv("DATABASE")
    username=os.getenv("USERNAME")
    password=os.getenv("PASSWORD")
    try :
        conn=pymssql.connect(hostname, username, password, database)
    except:
        raise Exception("Connection failed!")
    return conn

def create_jobs_table(conn):
    script = """
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='jobs' AND xtype='U')
        CREATE TABLE jobs (
            ID INT IDENTITY(1,1) PRIMARY KEY,
            PROJECT_ID INT,
            SOURCE VARCHAR(255),
            TYPE VARCHAR(100),
            TITLE VARCHAR(1000),
            SERVICES VARCHAR(1000),
            SKILLS VARCHAR(1000),
            DESCRIPTION TEXT,
            DATE_POSTED DATETIME,
            REMAINING_DAYS INT,
            LOCATION VARCHAR(255),
            BUDGET_MIN FLOAT,
            BUDGET_MAX FLOAT,
            BUDGET_CURRENCY VARCHAR(10),
            WORKING_TYPE VARCHAR(100),
            PAYMENT_TYPE VARCHAR(100),
            BID_COUNT INT,
            LOWEST_BID FLOAT,
            AVERAGE_BID FLOAT,
            HIGHEST_BID FLOAT,
            DURATION INT,
            URL VARCHAR(2083)
        );
    """
    cursor = conn.cursor()
    cursor.execute(script)
    conn.commit()
    cursor.close()
def create_exchange_rate_table(conn):
    script = """
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='exchange_rate' AND xtype='U')
        CREATE TABLE exchange_rate (
            BASE_CURRENCY VARCHAR(10),
            TARGET_CURRENCY VARCHAR(10),
            DATE DATETIME,
            RATE FLOAT,
            PRIMARY KEY (BASE_CURRENCY, TARGET_CURRENCY, DATE)
        );
    """
    cursor = conn.cursor()
    cursor.execute(script)
    conn.commit()
    cursor.close()

def handle_exchange_rate(p_date:str,base_currency='vnd',target_currency='usd'):
    url="https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@{}/v1/currencies/{}.json".format(p_date,base_currency)
    try: 
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.HTTPError as errh:
        print ("Http Error:",errh)
    except requests.exceptions.ConnectionError as errc:
        print ("Error Connecting:",errc)
    except requests.exceptions.Timeout as errt:
        print ("Timeout Error:",errt)
    return (data['date'],base_currency,target_currency,data[base_currency][target_currency])

# file_path is the jobs csv file, load this file to get min date_posted
def load_exchange_rate_to_db(conn,is_full_load=True):
    print("[INFO] Crawling exchange rate")
    # Get min date_posed from jobs table
    cusor = conn.cursor()
    cusor.execute("SELECT min(date_posted) FROM jobs")
    min_date_posted = cusor.fetchall()
    min_date_posted = min_date_posted[0][0]
    cusor.close()
    # Convert min_date_posted to YYYY-MM-DD 
    max_date_posted = datetime.now()
    print(type(min_date_posted),type(max_date_posted))
    
    cursor = conn.cursor()
    # TRUNCATE TABLE exchange_rate; if is_full_load=True
    if is_full_load:
        cursor.execute("TRUNCATE TABLE exchange_rate;")
    while min_date_posted<max_date_posted:
        p_date=min_date_posted.strftime('%Y-%m-%d')
        print(p_date)
        date,base_currency,target_currency,rate=handle_exchange_rate(p_date)
        script_insert="""
            INSERT INTO exchange_rate (
                BASE_CURRENCY, TARGET_CURRENCY, DATE, RATE
            ) VALUES (%s,%s,TO_TIMESTAMP(%s, 'YYYY-MM-DD'),%s)
        """
        cursor.execute(script_insert, (str.upper(base_currency), str.upper(target_currency), date, rate))
        min_date_posted=min_date_posted+timedelta(days=1)
    
    conn.commit()
    cursor.close()
def create_users_table(conn):
    script = """
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='users' AND xtype='U')
        CREATE TABLE users (
            ID INT IDENTITY(1,1) PRIMARY KEY,
            USER_ID INT,
            SOURCE VARCHAR(255),
            REGION VARCHAR(255),
            OVERVIEW TEXT,
            SERVICES VARCHAR(1000),
            RATINGS DOUBLE PRECISION,
            REVIEW_COUNT FLOAT,
            COMPLETION_RATE FLOAT,
            REHIRE_RATE FLOAT,
            EXPERIENCE NVARCHAR(255),
            URL VARCHAR(2083)
        );
    """
    cursor = conn.cursor()
    cursor.execute(script)
    conn.commit()
    cursor.close()
def read_data_from_csv(file_path):

    
    df = pd.read_csv(file_path)
    return df
def convert_to_None(df):
    
    # Replace Nan with None
    for col in df.columns:
        if df[col].dtype == np.float64:
            df[col] = df[col].apply(lambda x: None if pd.isnull(x) else x)
        if df[col].dtype == np.int64:
            df[col] = df[col].apply(lambda x: None if pd.isnull(x) else x)
    return df
def replace_nan_with_none(value):
    if isinstance(value, float) and math.isnan(value):
        return None
    return value
def load_jobs_to_db(conn, file_path,is_full_load=False):
    jobs_df=read_data_from_csv(file_path)
    cursor = conn.cursor()
    # TRUNCATE TABLE jobs; if is_full_load=True
    if is_full_load:
        print("[INFO] Truncate table jobs")
        cursor.execute("TRUNCATE TABLE jobs;")
        
    # Get PROJECT_ID from jobs table to check if job_id exists
    cursor.execute("SELECT PROJECT_ID FROM jobs WHERE SOURCE='freelancer'")
    job_ids = cursor.fetchall()
    job_ids = [job_id[0] for job_id in job_ids]
    for i, row in jobs_df.iterrows():
       
        if row['job_id'] in job_ids:
    
            script_update="""
                UPDATE jobs
                SET TITLE=%s, SERVICES=%s, SKILLS=%s, DESCRIPTION=%s, DATE_POSTED=CONVERT(DATETIME, %s, 103), 
                REMAINING_DAYS=%s, BUDGET_MIN=%s, BUDGET_MAX=%s, BUDGET_CURRENCY=%s, BID_COUNT=%s, AVERAGE_BID=%s, URL=%s
                WHERE PROJECT_ID=%s AND SOURCE='freelancer'
            """
            values=(row['title'], str(row['services']), str(row['skills']), 
                                        row['description'], row['date_posted'], 7,
                                        row['budget_min'], row['budget_max'], row['budget_currency'],
                                        row['bid_count'], row['average_bid'], row['url'], row['job_id'])
            values = tuple(replace_nan_with_none(v) for v in values)
            cursor.execute(script_update, values)
        else:
            script_insert="""
                INSERT INTO jobs (
                    PROJECT_ID,SOURCE, TYPE, TITLE,SERVICES,SKILLS,DESCRIPTION,DATE_POSTED,\
                    REMAINING_DAYS,BUDGET_MIN,BUDGET_MAX,BUDGET_CURRENCY,BID_COUNT,AVERAGE_BID,URL
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,CONVERT(DATETIME, %s, 103),%s,%s,%s,%s,%s,%s,%s)
            """
            values=(row['job_id'], 'freelancer',row['type'],row['title'],str(row['services']),str(row['skills']),\
                    row['description'],row['date_posted'],7,row['budget_min'],row['budget_max'],row['budget_currency'],\
                    row['bid_count'],row['average_bid'],row['url']
            )
            values = tuple(replace_nan_with_none(v) for v in values)
            cursor.execute(script_insert, values)
    conn.commit()
    cursor.close()
def load_users_to_db(conn, file_path,is_full_load=True):
    users_df=read_data_from_csv(file_path)
    cursor = conn.cursor()
    # TRUNCATE TABLE users; if is_full_load=True
    if is_full_load:
        cursor.execute("TRUNCATE TABLE users;")
        print("[INFO] Truncate table users")
    

    # Get USER_ID from users table to check if user_id exists
    print("[INFO] Getting list of user_id to check if new user_id exists")
    cursor.execute("SELECT USER_ID FROM users")
    user_ids = cursor.fetchall()
    user_ids = [user_id[0] for user_id in user_ids]
    
    print("[INFO] Loading users to db")
    for i, row in users_df.iterrows():
        if row['user_id'] in user_ids:
            script_update="""
                UPDATE users
                SET REGION=%s, OVERVIEW=%s, RATINGS=%s, URL=%s
                WHERE USER_ID=%s
            """
            values=(row['region'], row['overview'], row['ratings'], row['url'], row['user_id'])
            values = tuple(replace_nan_with_none(v) for v in values)
            cursor.execute(script_update, values)
        else:
            script_insert="""
                INSERT INTO users (
                    USER_ID, SOURCE, REGION, OVERVIEW, RATINGS, URL
                ) VALUES (%s,%s,%s,%s,%s,%s)
            """
            values=(row['user_id'], 'freelancer', row['region'], row['overview'], row['ratings'], row['url'])
            values = tuple(replace_nan_with_none(v) for v in values)
            cursor.execute(script_insert, values)
    
    conn.commit()
    cursor.close()
def create_applicants_table(conn):
    script = """
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='applicants' AND xtype='U')
        CREATE TABLE applicants (
            JOB_ID INT,
            BID_ID INT,
            SOURCE VARCHAR(255),
            PRIMARY KEY (JOB_ID, BID_ID, SOURCE)
        );
    """
    cursor = conn.cursor()
    cursor.execute(script)
    conn.commit()
    cursor.close()
    
def load_applicants_to_db(conn,file_in,is_full_load=True):
    applicants_df=read_data_from_csv(file_in)
    applicants_df=applicants_df.drop_duplicates()
    # Delete if null 
    applicants_df=applicants_df.dropna(subset=['bid_id'])
    applicants_df['bid_id']=applicants_df['bid_id'].astype(int)
    
    
    cursor = conn.cursor()
    # TRUNCATE TABLE applicants; if is_full_load=True
    if is_full_load:
        cursor.execute("TRUNCATE TABLE applicants;")
        
    # Get list of JOB_ID, BID_ID,SOURCE from applicants table to check if job_id, bid_id,source exists
    cursor.execute("SELECT JOB_ID, BID_ID,SOURCE FROM applicants WHERE SOURCE='freelancer'")
    job_bid_ids = cursor.fetchall()
    job_bid_ids = [(job_bid_id[0],job_bid_id[1],job_bid_id[2]) for job_bid_id in job_bid_ids]

    for i, row in applicants_df.iterrows():
        script_insert="""
            INSERT INTO applicants (
                JOB_ID, BID_ID,SOURCE
            ) VALUES (%s,%s,%s)
        """
        if (int(row['job_id']), int(row['bid_id']), 'freelancer') in job_bid_ids:
            continue
        else:
            # Conver to native int type
            try: 
                cursor.execute(script_insert, (int(row['job_id']), int(row['bid_id']), 'freelancer'))
            except Exception as e:
                print(f"[WARNING] {e}")
                print(f"[WARNING] {row['job_id']} {row['bid_id']}")
                pass 
    
    conn.commit()
    cursor.close()
    
def main():
    parser = argparse.ArgumentParser(description="Example of optional arguments")
    parser.add_argument("--post_prefix", type=str, help="Your post prefix file name", default=date.today().strftime("%d%m%Y"))
    parser.add_argument("--full_load", type=bool, help="Is full load ?", default=False)
    args = parser.parse_args()    

    base_path_processed = Path('../../../data/processed/')
    conn = connect_to_db()
    print("[INFO] Connected to database")
    
    # Create table if not exists
    create_jobs_table(conn)
    create_users_table(conn)
    create_exchange_rate_table(conn)
    create_applicants_table(conn)
    
    
    # Load jobs from csv to db
    print("[INFO] Loading jobs to db")
    file_jobs_in=base_path_processed / f'freelancer_jobs_{args.post_prefix}.csv'
    load_jobs_to_db(conn, file_jobs_in,is_full_load=args.full_load)
    print("[INFO] Jobs loaded to db")
    
    # Load users from csv to db
    print("[INFO] Loading users to db")
    file_users_in=base_path_processed / f'freelancer_users_{args.post_prefix}.csv'
    load_users_to_db(conn, file_users_in,is_full_load=args.full_load)
    print("[INFO] Users loaded to db")
    
    # # Load applicants from csv to db
    # print("[INFO] Loading applicants to db")
    # file_applicants_in=base_path_processed / f'freelancer_bidder_jobs_{args.post_prefix}.csv'
    # load_applicants_to_db(conn, file_applicants_in,is_full_load=args.full_load)
    # print("[INFO] Applicants loaded to db")
    
    # # Load exchange rate to db
    # print("[INFO] Loading exchange rate to db")
    # load_exchange_rate_to_db(conn, file_jobs_in,is_full_load=True)
    # print("[INFO] Exchange rate loaded to db")
    
    conn.close()
    print("[INFO] Disconnected from database")
if __name__ == "__main__":
    main()
    