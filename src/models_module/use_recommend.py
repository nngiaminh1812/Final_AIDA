import pandas as pd
import numpy as np
from sentence_transformers import SentenceTransformer, util
import pickle
import torch 
import pymssql 
from dotenv import load_dotenv
import os


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
    return conn
def get_num_bids():
    conn=connect_to_db()
    cursor = conn.cursor()
    query="""
        select job_id,count(*)
        from applicants
        group by job_id 
    """
    cursor.execute(query)
    num_bids = cursor.fetchall()
    conn.close()
    return num_bids
def load_embeddings_from_file(filepath):
    with open(filepath, "rb") as f:
        data = pickle.load(f)
    return pd.DataFrame(data)
def recommend_jobs(df, user_input, model, skills_weight=0.5, description_weight=0.5):
    # Encode user input
    user_skills_embedding = model.encode(user_input['skills'], convert_to_tensor=False)
    user_description_embedding = model.encode(user_input['description'], convert_to_tensor=False)


    # Compute similarity
    df['skills_similarity'] = df['skills_embedding'].apply(
        lambda x: util.cos_sim(user_skills_embedding, x).item()
    )
    df['description_similarity'] = df['description_embedding'].apply(
        lambda x: util.cos_sim(user_description_embedding, x).item()
    )

    # Combine scores with weights
    df['total_score'] = (skills_weight * df['skills_similarity'] +
                         description_weight * df['description_similarity'])

    # Filter by budget if provided
    if user_input['budget_min'] is not None and user_input['budget_max'] is not None:
        df = df[
            (df['budget_min'] <= user_input['budget_max']) & 
            (df['budget_max'] >= user_input['budget_min'])
        ]

    # Sort by total score and return recommendations
    recommended_jobs = df.sort_values(by='total_score', ascending=False)
    
    
    # Add number of bids
    num_bids=get_num_bids()
    df_num_bids=pd.DataFrame(num_bids,columns=['project_id','num_bids'])
    # Merge with recommended_jobs
    recommended_jobs = recommended_jobs.merge(df_num_bids, left_on='project_id', right_on='project_id', how='left')
    # Remove index 
    recommended_jobs.reset_index(drop=True, inplace=True)
    return recommended_jobs[['total_score','num_bids',
         'title', 'services', 'skills', 'description', 
        'budget_min', 'budget_max', 'url'
    ]]

# Tải embedding từ file
# def recommend():
#     filepath = "src\models_module\embedded_vector.pkl"
#     df = load_embeddings_from_file(filepath)
#     model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')


#     user_input = {
#         "skills": " Unity, C#",  # Kỹ năng cá nhân
#         "description": "I have experience in programming C# and Unity engine",  # Mô tả bản thân
#         "budget_min": 10,  
#         "budget_max": 1110   
#     }
#     recommendations = recommend_jobs(df, user_input, model)