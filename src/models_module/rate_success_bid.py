from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity

import pandas as pd
import sys
import os
src_path = os.path.abspath(
    "/run/media/nngiaminh1812/Data/IDA/Final_AIDA/src"
)
if src_path not in sys.path:
    sys.path.append(src_path)

# Import the sql_query module
from visualize_module.webapp.sql_query import connect_to_db


model = SentenceTransformer('sentence-transformers/all-mpnet-base-v2')
# Function to get data from the database
def get_data_from_db(job_id):
    conn = connect_to_db()
    sql_query = f"SELECT DESCRIPTION, SERVICES, SKILLS FROM jobs WHERE PROJECT_ID={job_id}"
    df = pd.read_sql(sql_query, conn)
    description = df['DESCRIPTION'].iloc[0]
    services = df['SERVICES'].iloc[0]
    skills = df['SKILLS'].iloc[0]

    combined_text = (
    f"{description}\n"
    f"Services required:{services}\n"
    f"Skills required:{skills}"
        )
    conn.close()

    print(description, services, skills)
    return combined_text

# Function to predict bid success rate based on sentence similarity
def predict_bid_success(job_id, freelancer_overview, freelancer_services):
    # Combine the freelancer's overview and services
    job_in4 = get_data_from_db(job_id)
    freelancer_text = freelancer_overview + " " + freelancer_services
    
    # Encode the job description and freelancer text using the SentenceTransformer model
    job_embedding = model.encode([job_in4])
    freelancer_embedding = model.encode([freelancer_text])
    
    # Compute the cosine similarity between the job description and freelancer text
    similarity_score = cosine_similarity(job_embedding, freelancer_embedding)[0][0]
    
    return similarity_score

