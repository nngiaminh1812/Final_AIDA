
import pandas as pd
import numpy as np
import pickle
import pyodbc
from sentence_transformers import SentenceTransformer
import sys


def save_embeddings_to_file(filepath, df, model):
    data = []
    i = 0
    for _, job in df.iterrows():
        sys.stdout.write(f"\r{i}")
        sys.stdout.flush()
        i+=1
        # Encode skills và description
        skills_embedding = model.encode(job['skills'], convert_to_tensor=False).tolist()
        description_embedding = model.encode(job['description'], convert_to_tensor=False).tolist()

        # Lưu vào danh sách
        data.append({
            "project_id": job['project_id'],
            # "owner_id": job['owner_id'],
            "type": job['type'],
            "title": job['title'],
            "services": job['services'],
            "skills": job['skills'],
            "description": job['description'],
            "date_posted": job['date_posted'],
            "budget_min": job['budget_min'],
            "budget_max": job['budget_max'],
            "bid_count": job['bid_count'],
            "average_bid": job['average_bid'],
            # "status": job['status'],
            "url": job['url'],
            "skills_embedding": skills_embedding,
            "description_embedding": description_embedding
        })
    
    # Ghi danh sách vào file
    with open(filepath, "wb") as f:
        pickle.dump(data, f)




def build_embeddedvector():
   
    filepath = "src/models_module/embedded_vector.pkl"
    # Thông tin kết nối Azure SQL
    server = 'teamaidaserver.database.windows.net'
    database = 'aida_db'  
    username = 'minhtri' 
    password = 'a123456789B'  
    driver = '{ODBC Driver 18 for SQL Server}' 

    # Kết nối tới Azure SQL
    connection_string = f'DRIVER={driver};SERVER={server};PORT=1433;DATABASE={database};UID={username};PWD={password}'
    conn = pyodbc.connect(connection_string)

    
    query = "SELECT * FROM dbo.jobs"  
    df = pd.read_sql(query, conn)
    df.columns = df.columns.str.lower()
    print(df.columns)
    print("Get data from Database successs!!!!!!!!!")

    conn.close()

    # Đảm bảo các cột số được chuyển thành số
    df['budget_min'] = pd.to_numeric(df['budget_min'], errors='coerce')
    df['budget_max'] = pd.to_numeric(df['budget_max'], errors='coerce')
    print("Read success!")

    model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')


    # Lưu vector embedding
    save_embeddings_to_file(filepath, df, model)
    print("Save embedding vectors to file success!")


#  UNLOCK MAIN TO RUN 
def main():
    print("Starting the embedding generation process...")
    build_embeddedvector()
    print("Process completed successfully!")

if __name__ == "__main__":
    main()