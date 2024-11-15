import sqlite3
import csv

db_file = '/home/nngiaminh1812/airflow/vlance_db.sqlite'
conn = sqlite3.connect(db_file)

cursor = conn.cursor()

# create_table_query = '''
# CREATE TABLE IF NOT EXISTS jobs (
#     "Project ID" TEXT PRIMARY KEY,
#     "Type" TEXT,
#     "Title" TEXT,
#     "Services" TEXT,
#     "Skills" TEXT,
#     "Description" TEXT,
#     "Posted Date" TEXT,
#     "Remaining Days" INTEGER,
#     "Location" TEXT,
#     "Minimum Budget" REAL,
#     "Maximum Budget" REAL,
#     "Working Type" TEXT,
#     "Payment Type" TEXT,
#     "Num_applicants" INTEGER,
#     "Lowest Bid" REAL,
#     "Average Bid" REAL,
#     "Highest Bid" REAL,
#     "Duration (Days)" INTEGER,
#     "Link" TEXT
# );
# '''

# cursor.execute(create_table_query)

csv_file = '/home/nngiaminh1812/airflow/dags/new_cleaned_jobs.csv'

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