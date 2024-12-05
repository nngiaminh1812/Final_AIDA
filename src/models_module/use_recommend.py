import pandas as pd
import numpy as np
from sentence_transformers import SentenceTransformer, util
import pickle
import torch 

def load_embeddings_from_file(filepath):
    with open(filepath, "rb") as f:
        data = pickle.load(f)
    return pd.DataFrame(data)
def recommend_jobs(df, user_input, model, skills_weight=0.5, description_weight=0.5):
    # Encode user input
    user_skills_embedding = model.encode(user_input['skills'], convert_to_tensor=False)
    user_description_embedding = model.encode(user_input['description'], convert_to_tensor=False)

    print(len(user_skills_embedding))  # Kích thước vector
    print(len(df['skills_embedding'][0])) 
    print(len(user_description_embedding))  # Kích thước vector
    print(len(df['description_embedding'][0])) 
    print(df.columns)

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
    recommended_jobs = df.sort_values(by='total_score', ascending=False).head(5)
    return recommended_jobs[[ 'project_id',
         'title', 'services', 'skills', 'description', 
        'budget_min', 'budget_max', 'total_score', 'url'
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