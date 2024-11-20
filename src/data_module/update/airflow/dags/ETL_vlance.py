import sqlite3
import pandas as pd
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import date
import csv

def load_task():
    conn = sqlite3.connect('/home/nngiaminh1812/airflow/vlance_db.sqlite')
    cursor = conn.cursor()
    
    csv_file = f'/home/nngiaminh1812/airflow/dags/data/preprocessed/new_cleaned_jobs_{date.today().strftime("%Y-%m-%d")}.csv'
    with open(csv_file, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        
        # insert samples to table
        for row in reader:
            cursor.execute('''
                INSERT OR IGNORE INTO jobs (
                    "Project ID", "Type", "Title", "Services", "Skills", "Description", "Posted Date", "Remaining Days", 
                    "Location", "Minimum Budget", "Maximum Budget", "Working Type", "Payment Type", "Num_applicants", 
                    "Lowest Bid", "Average Bid", "Highest Bid", "Duration (Days)", "Link"
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                row['Project ID'], row['Type'], row['Title'], row['Services'], row['Skills'], row['Description'], 
                row['Posted Date'], row['Remaining Days'], row['Location'], row['Minimum Budget'], row['Maximum Budget'], 
                row['Working Type'], row['Payment Type'], row['Num_applicants'], row['Lowest Bid'], row['Average Bid'], 
                row['Highest Bid'], row['Duration (Days)'], row['Link']
            ))

    conn.commit()

    conn.close()


default_args = {
    'owner': 'admin',
    'start_date': days_ago(1),
    'retries': 0,
}

with DAG('etl_vlance_dag', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    update = BashOperator(
        task_id='update_vlance_jobs',
        bash_command='/home/nngiaminh1812/.pyenv/versions/3.11.0/envs/airflow_env/bin/python /home/nngiaminh1812/airflow/dags/jobs_vlance_update.py'
    )

    preprocess = BashOperator(
        task_id='preprocess_jobs',
        bash_command='/home/nngiaminh1812/.pyenv/versions/3.11.0/envs/airflow_env/bin/python /home/nngiaminh1812/airflow/dags/preprocess_new_job.py'
    )

    load = PythonOperator(
        task_id='load_jobs_to_db',
        python_callable=load_task
    )

    user_update = TriggerDagRunOperator(
        task_id='trigger_user_vlance_dag',
        trigger_dag_id='user_vlance_dag',
    )
    update >> preprocess >> load >> user_update