import sqlite3
import pandas as pd
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

# 3 tasks in dag include update, preprocess, and load
# def update_task():
#     update_vlance_jobs("qn9965mh@gmail.com", "123456asd*")

# def preprocess_task():
#     preprocess_jobs()

def load_task():
    conn = sqlite3.connect('/home/nngiaminh1812/airflow/vlance_db.sqlite')
    df = pd.read_csv('/home/nngiaminh1812/airflow/dags/new_cleaned_jobs.csv')
    df.to_sql('jobs', conn, if_exists='append', index=False)
    conn.close()

default_args = {
    'owner': 'admin',
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG('etl_vlance_dag', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    update = BashOperator(
        task_id='update_vlance_jobs',
        bash_command='/home/nngiaminh1812/.pyenv/versions/3.11.0/envs/airflow_env/bin/python /home/nngiaminh1812/airflow/dags/vlance_update.py'
    )

    preprocess = BashOperator(
        task_id='preprocess_jobs',
        bash_command='/home/nngiaminh1812/.pyenv/versions/3.11.0/envs/airflow_env/bin/python /home/nngiaminh1812/airflow/dags/preprocess_new_job.py'
    )

    load = PythonOperator(
        task_id='load_jobs_to_db',
        python_callable=load_task
    )

    update >> preprocess >> load