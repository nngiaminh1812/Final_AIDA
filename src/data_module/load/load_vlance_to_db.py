import csv
from dotenv import load_dotenv
import pymssql
import os
import argparse
from datetime import date
import subprocess
from datetime import datetime


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

def load_jobs_to_db(input_path):

    conn = connect_to_db()
    cursor = conn.cursor()
    cursor.execute("SELECT PROJECT_ID FROM jobs WHERE SOURCE='vlance'")
    job_ids = cursor.fetchall()
    job_ids = [job_id[0] for job_id in job_ids]
    with open(input_path, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            if row['Project ID'] not in job_ids:
                script_insert="""
                    INSERT INTO jobs (
                        PROJECT_ID,SOURCE, TYPE, TITLE,SERVICES,SKILLS,DESCRIPTION,DATE_POSTED,\
                        REMAINING_DAYS,LOCATION,BUDGET_MIN,BUDGET_MAX,BUDGET_CURRENCY,WORKING_TYPE,PAYMENT_TYPE,\
                        BID_COUNT,LOWEST_BID,AVERAGE_BID,HIGHEST_BID,DURATION,URL
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """
                values=(row['Project ID'],'vlance',row['Type'],row['Title'],\
                        row['Services'],row['Skills'],row['Description'],datetime.strptime(row['Posted Date'][:10:], "%d/%m/%Y"),\
                        row['Remaining Days'],row['Location'],row['Minimum Budget'],row['Maximum Budget'],\
                        'VND',row['Working Type'],row['Payment Type'],row['Num_applicants'],row['Lowest Bid'],\
                        row['Average Bid'],row['Highest Bid'],row['Duration (Days)'],row['Link'])
                cursor.execute(script_insert,values)
            else:
                script_update="""
                    UPDATE jobs
                    SET BID_COUNT=%s,LOWEST_BID=%s,AVERAGE_BID=%s,HIGHEST_BID=%s
                    WHERE PROJECT_ID=%s
                """
                values=(row['Num_applicants'],row['Lowest Bid'],row['Average Bid'],row['Highest Bid'],row['Project ID'])
                cursor.execute(script_update,values)
            
    conn.commit()
    conn.close()
def load_users_to_db(input_path):
    conn = connect_to_db()
    cursor = conn.cursor()
    cursor.execute("SELECT USER_ID FROM users WHERE SOURCE='vlance'")
    user_ids = cursor.fetchall()
    user_ids = [user_id[0] for user_id in user_ids]
    with open(input_path, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            if row['id'] not in user_ids:
                script_insert="""
                    INSERT INTO users (
                        USER_ID, SOURCE, REGION, OVERVIEW,SERVICES,RATINGS,REVIEW_COUNT,COMPLETION_RATE,REHIRE_RATE,EXPERIENCE
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """
                values=(row['id'],'vlance',row['region'],row['overview'],row['services'],\
                        row['rating'],row['review_count'],row['completion_rate'],row['rehire_rate'],\
                        row['experience'])
                cursor.execute(script_insert,values)
            else:
                script_update="""
                    UPDATE users
                    SET RATING=%s,REVIEW_COUNT=%s,COMPLETION_RATE=%s,REHIRE_RATE=%s
                    WHERE USER_ID=%s
                """
                values=(row['rating'],row['review_count'],row['completion_rate'],row['rehire_rate'],row['User ID'])
                cursor.execute(script_update,values)
    conn.commit()
    conn.close()
def load_applications_to_db(input_path):
    conn=connect_to_db()
    cursor=conn.cursor()
    # Get list of JOB_ID, BID_ID,SOURCE from applicants table to check if job_id, bid_id,source exists
    cursor.execute("SELECT JOB_ID, BID_ID, SOURCE FROM applicants WHERE SOURCE='vlance'")
    job_bid_ids = cursor.fetchall()
    job_bid_ids = [(job_bid_id[0],job_bid_id[1],job_bid_id[2]) for job_bid_id in job_bid_ids]
    with open(input_path,'r',encoding='utf-8') as file:
        reader=csv.DictReader(file)
        for row in reader:
            if (int(row['id_project']),int(row['id']),'vlance') not in job_bid_ids:
                script_insert="""
                    INSERT INTO applicants (
                    JOB_ID, BID_ID,SOURCE) 
                    VALUES (%s,%s,%s)
                """
                values=(int(row['id_project']),int(row['id']),'vlance')
                try:
                    cursor.execute(script_insert,values)
                except:
                    continue
            else:
                continue
    conn.commit()
    conn.close()
# def migration_applicants(input_path):
#     conn=connect_to_db()
#     with open(input_path,'r',encoding='utf-8') as file:
#         reader=csv.DictReader(file)
#         num_batch=1000
#         batch=0
#         for batch in range(0,num_batch):
#             data=[]
#             cursor=conn.cursor()
#             for row in reader[batch:batch+num_batch]:
#                 data.append((int(row['JOB_ID']),int(row['BID_ID']),row['SOURCE']))
#             script_insert="""
#                 INSERT INTO applicants (
#                 JOB_ID, BID_ID,SOURCE) 
#                 VALUES (%s,%s,%s)
#             """
#             cursor.executemany(script_insert,data)
#             conn.commit()
#     conn.close()
def main():
    parser = argparse.ArgumentParser(description="Example of optional arguments")
    parser.add_argument("--post_prefix", type=str, help="Your post prefix file name", default=date.today().strftime("%Y%m%d"))
    parser.add_argument("--is_translate", type=str, help="Do you need translate vienamese to english before", default=False)
    args = parser.parse_args()
    if args.is_translate:
        try:
            print("[INFO] Translating jobs and users")
            subprocess.run(["python", "../processing/vlance_translate.py", "--post_prefix", args.post_prefix])
        except:
            raise Exception("Failed to translate")
    
    # input_path = "../../../data/processed/vlance_jobs_{}.csv".format(args.post_prefix)

    # print("[INFO] Loading jobs to database")
    # try :
    #     load_jobs_to_db(input_path)
    # except:
    #     raise Exception("Failed to load jobs to database")
    # print("[INFO] Loaded jobs to database")
    
    # input_path = "../../../data/processed/vlance_users_{}.csv".format(args.post_prefix)
    # print("[INFO] Loading users to database")
    # try:
    #     load_users_to_db(input_path)
    # except:
    #     raise Exception("Failed to load users to database")
    # print("[INFO] Loaded users to database")
    
    input_path = "../../../data/processed/vlance_applicants_{}.csv".format(args.post_prefix)
    print("[INFO] Loading applications to database")
    try:
        load_applications_to_db(input_path)
    except:
        raise Exception("Failed to load applications to database")
    print("[INFO] Loaded applications to database")
    print("[INFO] Done!")
if __name__=="__main__":
    main()