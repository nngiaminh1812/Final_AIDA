import sqlite3
import pandas as pd
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from datetime import date
import csv


def load_task():
    conn = sqlite3.connect('/home/nngiaminh1812/airflow/vlance_db.sqlite')
    cursor = conn.cursor()
    create_applicants_table = '''
    CREATE TABLE IF NOT EXISTS applicants (
        "id" TEXT PRIMARY KEY,
        "title" TEXT,
        "region" TEXT,
        "overview" TEXT,
        "services" TEXT,
        "rating" REAL,
        "review_count" INTEGER,
        "completion_rate" REAL,
        "rehire_rate" REAL,
        "experience" TEXT
    );
'''

    create_applications_table = '''
    CREATE TABLE IF NOT EXISTS applications (
        "id" TEXT,
        "id_project" TEXT,
        PRIMARY KEY (id, id_project)
    ); 
'''

    cursor.execute(create_applicants_table)
    cursor.execute(create_applications_table)
    
    with open(f'/home/nngiaminh1812/airflow/dags/data/preprocessed/new_users_vlance_{date.today().strftime("%Y-%m-%d")}.csv', 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        
        # insert samples to table
        for row in reader:
            cursor.execute('''
                INSERT OR IGNORE INTO applicants (
                    "id", "title", "region", "overview", "services", "rating", "review_count", "completion_rate", "rehire_rate", "experience"
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                row['id'], row['title'], row['region'], row['overview'], row['services'], row['rating'], 
                row['review_count'], row['completion_rate'], row['rehire_rate'], row['experience']
            ))

    with open(f'/home/nngiaminh1812/airflow/dags/data/preprocessed/new_applications_vlance_{date.today().strftime("%Y-%m-%d")}.csv', 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        
        # insert samples to table
        for row in reader:
            cursor.execute('''
                INSERT OR IGNORE INTO applications (
                    "id", "id_project"
                ) VALUES (?, ?)
            ''', (
                row['id'], row['id_project']
            ))

    conn.commit()
    conn.close()

default_args = {
    'owner': 'admin',
    'start_date': days_ago(1),
    'retries': 0,
}

with DAG('user_vlance_dag', default_args=default_args, schedule_interval=None, catchup=False) as dag:

    update = BashOperator(
        task_id='update_users_vlance',
        bash_command='/home/nngiaminh1812/.pyenv/versions/3.11.0/envs/airflow_env/bin/python /home/nngiaminh1812/airflow/dags/user_vlance_update.py'
    )

    preprocess = BashOperator(
        task_id='preprocess_users_vlance',
        bash_command='/home/nngiaminh1812/.pyenv/versions/3.11.0/envs/airflow_env/bin/python /home/nngiaminh1812/airflow/dags/preprocess_user.py'
    )

    load = PythonOperator(
        task_id='load_applies_to_db',
        python_callable=load_task
    )

    update >> preprocess >> load