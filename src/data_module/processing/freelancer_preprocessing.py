import pandas as pd
from pathlib import Path
import pandas as pd
import argparse
from datetime import datetime
import os 
from dotenv import load_dotenv
import glob
from datetime import date
import pymssql
# Function to convert a comma-separated string into a list
def convert_to_list(skills_str):
    if isinstance(skills_str, str):
        return skills_str.split(',')
    return skills_str
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

def preprocessing_jobs(file_in,file_out, file_out_bidder):
    """
    Preprocess the freelancer jobs data
    """
    # Load the data
    print("[INFO] Loading the data")
    jobs_df = pd.read_csv(file_in)
    
    print("[INFO] Preprocessing the data")
    
    # Change dtype
    jobs_df['job_id'] = jobs_df['job_id'].astype('category')
    jobs_df['owner_id'] = jobs_df['owner_id'].astype('category')
   
    # Remove all rows have language except English
    jobs_df=jobs_df[jobs_df['language']=='en']

    # Remove columns language 
    jobs_df=jobs_df.drop(columns=['language'])
    
    print("[INFO] Removing duplicates")
    # Check for duplicate rows
    duplicates = jobs_df[jobs_df.duplicated()]

    # Print the duplicate rows
    print(f"[INFO] Number of duplicate rows: {len(duplicates)}")
    jobs_df = jobs_df.drop_duplicates()
    
    # Drop duplicate in 'job_id' columns 
    jobs_df = jobs_df.drop_duplicates(subset=["job_id"], keep="last")
    
    # By default, the jobs in freelancer web expire after 7 days
    jobs_df['date_exprire']=jobs_df['date_posted']+7*86400

    # Convert date_posted and date_exprire datetime
    jobs_df['date_posted'] = pd.to_datetime(jobs_df['date_posted'], unit='s')
    jobs_df['date_posted'] = jobs_df['date_posted'].dt.strftime('%d/%m/%Y %H:%M:%S')

    jobs_df['date_exprire'] = pd.to_datetime(jobs_df['date_exprire'], unit='s')
    jobs_df['date_exprire'] = jobs_df['date_exprire'].dt.strftime('%d/%m/%Y %H:%M:%S')

    # Remove the time_remaining columns that are not needed
    jobs_df = jobs_df.drop(columns=['time_remaining'])

    
    # Convert to USD
    jobs_df['budget_min'] = jobs_df['budget_min'] * jobs_df['exchange_rate'] 
    jobs_df['budget_max'] = jobs_df['budget_max'] * jobs_df['exchange_rate'] 
    jobs_df['average_bid'] = jobs_df['average_bid'] * jobs_df['exchange_rate'] 
    jobs_df['budget_currency']='USD'
    #Drop the exchange_rate column
    jobs_df = jobs_df.drop(columns=['exchange_rate'])
    
    
    # Apply the function to the 'skills' column
    #jobs_df['skills'] = jobs_df['skills'].apply(convert_to_list)
    
    
    # Apply the function to the 'bid_list' column
    # Step 1: Convert all values in 'bid_list' to strings and remove brackets/quotes
    jobs_df['bid_list'] = jobs_df['bid_list'].astype(str).str.replace(r"[\[\]']", "", regex=True)

    # Step 2: Split each cleaned string by commas to create a list of bid_ids
    jobs_df['bid_list'] = jobs_df['bid_list'].apply(lambda x: x.split(','))

    # Step 3: Explode the 'bid_list' column to separate each bid_id into its own row
    exploded_df = jobs_df.explode('bid_list')

    # Rename the column for clarity
    exploded_df = exploded_df.rename(columns={'bid_list': 'bid_id'})

    # Select only the 'job_id' and 'bid_id' columns, remove duplicates, and reset the index
    bidder_jobs_df = exploded_df[['job_id', 'bid_id']].drop_duplicates().reset_index(drop=True)

    #DROP if bid_id=""
    bidder_jobs_df = bidder_jobs_df[bidder_jobs_df['bid_id'] != ""]
    bidder_jobs_df = bidder_jobs_df[bidder_jobs_df['job_id'] != ""]

    
    
    bidder_jobs_df = bidder_jobs_df.drop_duplicates()
    
    # Remove the 'bid_list' column from the original dataframe
    jobs_df = jobs_df.drop(columns=['bid_list'])
    
    # Check to PROJECT_ID in database 
    conn = connect_to_db()
    cur = conn.cursor()
    cur.execute("SELECT project_id FROM jobs")
    project_ids = cur.fetchall()
    project_ids = [project_id[0] for project_id in project_ids]
    # Remove all rows have project_id in project_ids
    jobs_df=jobs_df[~jobs_df['job_id'].isin(project_ids)]
    
    # Check to bid_id, job_id in database
    #cur.execute("SELECT bid_id, project_id FROM applicants")
    
    # save 
    bidder_jobs_df.to_csv(file_out_bidder, index=False,sep=',')
    jobs_df.to_csv(file_out, index=False,sep=',')
    print("[INFO] Data preprocessing completed")

def preprocessing_users(file_in,file_out):
    print("[INFO] Loading the data")
    users_df = pd.read_csv(file_in)
    
    print("[INFO] Preprocessing the data")
    # Check for duplicate rows
    duplicates = users_df[users_df.duplicated()]

    # Print the duplicate rows
    print(f"[INFO] Number of duplicate rows: {len(duplicates)}")
    users_df = users_df.drop_duplicates()
    # Drop duplicate in 'user_id' columns 
    users_df = users_df.drop_duplicates(subset=["user_id"], keep="last")
    
    # Change dtype
    users_df['user_id'] = users_df['user_id'].astype('category')
    
    # save
    users_df.to_csv(file_out, index=False,sep=',')
    print("[INFO] Data preprocessing completed")
def merge_files(post_prefix):
    list_jobs_path=glob.glob("../../../data/raw/*jobs*{}.csv".format(post_prefix))
    list_users_path=glob.glob("../../../data/raw/*users*{}.csv".format(post_prefix))
    jobs_df=pd.concat([pd.read_csv(file) for file in list_jobs_path])
    users_df=pd.concat([pd.read_csv(file) for file in list_users_path])
    jobs_df.to_csv("../../../data/raw/freelancer_jobs_{}.csv".format(post_prefix),index=False)
    users_df.to_csv("../../../data/raw/freelancer_users_{}.csv".format(post_prefix),index=False)
    
    
    #Delete all files in list_jobs_path and list_users_path
    for file in list_jobs_path:
        os.remove(file)
    for file in list_users_path:
        os.remove(file)
        
def main():
    parser = argparse.ArgumentParser(description="Example of optional arguments")
    parser.add_argument("--post_prefix", type=str, help="Your post prefix file name", default=date.today().strftime("%d%m%Y"))
    parser.add_argument("--merge_before", type=str, help="Do you need merge all file before preprocessing?", default=True)
    args = parser.parse_args()
    if args.merge_before:
        merge_files(args.post_prefix)
    
    base_path_raw = Path('../../../data/raw/')
    base_path_processed = Path('../../../data/processed/')
    # Define the input and output file paths
    file_jobs_in = base_path_raw / f'freelancer_jobs_{args.post_prefix}.csv'
    file_jobs_out =base_path_processed / f'freelancer_jobs_{args.post_prefix}.csv'
    file_out_bidder = base_path_processed / f'freelancer_bidder_jobs_{args.post_prefix}.csv'
    file_users_in = base_path_raw / f'freelancer_users_{args.post_prefix}.csv'
    file_users_out = base_path_processed / f'freelancer_users_{args.post_prefix}.csv'
    # Preprocess the data
    preprocessing_jobs(file_jobs_in,file_jobs_out, file_out_bidder)
    preprocessing_users(file_users_in,file_users_out)
    
    
if __name__=="__main__":
    
    main()
    