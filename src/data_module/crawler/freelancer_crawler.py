import requests
import re
from bs4 import BeautifulSoup
import json
import pandas as pd
import time
import os
import urllib.parse
import math
from datetime import datetime
from dotenv import load_dotenv
import pymssql
import argparse


# Load environment variables from .env file
try:
    load_dotenv()
    print("Environment variables loaded successfully.")
    access_token = os.getenv("FREELANCER_TOKEN")
    #access_token = 'dhhfhf'
    if access_token is None:
        raise Exception("No access token found.")
except:
    raise Exception("No .env file found.")


"""**Chiến lược cào dữ liệu trang `freelancer.com`**:
- Do trang web đăng rất nhiều dự án liên quan đến các lĩnh vực khác nên nhóm sẽ parse homepage của trang web lấy mỗi các pagejob thuộc các category mà trang web đã định nghĩa sẵn, dựa trên đó nhóm chọn ra 4 category liên quan đến IT đó là:
- Web(1).
- Mobile(2).
- Data(6).
- AI(7).
- Sau khi đã lấy được các pagejob của các category trên, nhóm sẽ tiếp tục parse từng pagejob để lấy thông tin cho từng job (id_job).
- Sau khi có các id_job nhóm sẽ cào dữ liệu cho các job này thông qua API. 
- Mỗi job sẽ chứa thông tin các bidder (người đặt giá), thông tin của bidder sẽ được lấy thông qua API.
- Các dữ liệu sẽ được cập nhật liên tục và để tránh cào lại dữ liệu cũ, nhóm sẽ lấy ra ngày jobs muộn nhất trong dữ liệu đã cào được và cào dữ liệu từ ngày đó trở về sau.
"""


def get_pagejob_for_categories():
    
    url = "https://www.freelancer.com/job/"
    soup=BeautifulSoup(requests.get(url).text, "html.parser")

    #  Định nghĩa các category cần lấy: 1 = web, 2 = Mobile, 6 = Data,  7 = AI
    categories = [2, 3, 6, 7]

    # Tạo một dict chứa các category và các link của các job trong category đó
    category_links = {category_id: [] for category_id in categories}


    for category_id in categories:
        category_section = soup.find('section', id=f'category-{category_id}')
        if category_section:
            # Tìm tất cả các job item trong category
            job_items = category_section.find_all('li')
            
            for job_item in job_items:
                # Tìm link của job
                job_link = job_item.find('a', class_='PageJob-category-link')
                if job_link:
                    # Lấy text của job link
                    job_text = job_link.get_text(strip=True)
                    job_count = re.search(r'\((\d+)\)', job_text)
                    
                    if job_count and int(job_count.group(1)) > 0:
                        job_href = job_link['href']
                        # Thêm link và số lượng job vào category_links
                        category_links[category_id].append((job_href, int(job_count.group(1))))

    file_path="../../../data/raw/category_links.json"
    # Lưu category_links vào file json
    with open(file_path, 'w', encoding='utf-8') as json_file:
        json.dump(category_links, json_file, ensure_ascii=False, indent=4)

    return file_path


def get_list_job_URL_of_each_category():
    file_pagejobs_link=get_pagejob_for_categories()
    with open(file_pagejobs_link, 'r', encoding='utf-8') as json_file:
        category_links = json.load(json_file)


    # Dic chứa các URL của từng category
    category_urls = {}

    # Duyệt qua từng category
    for category_id, jobs in category_links.items():
        urls = []  
        
        for job in jobs:
            link, job_count = job
            
            # Tính số trang lớn nhất cần lấy
            n_page = math.ceil(job_count / 50)  # 50 jobs cho mỗi trang
            
            # Tạo URL cho từng trang
            for page_i in range(1, n_page + 1):
                url = f"https://www.freelancer.com{link}?page={page_i}"
                urls.append(url)  
        
        # Thêm các URL vào category_urls
        category_urls[category_id] = urls

    listjobs_path="../../../data/raw/list_jobs_URL_of_each_category.json"
    # Lưu category_urls vào file json
    with open(listjobs_path, 'w', encoding='utf-8') as json_file:
        json.dump(category_urls, json_file, ensure_ascii=False, indent=4)



# Get the list of Job URLs from the url page in the list_jobs_URL_of_each_category.json
def get_job_atrributes(url):

    soup=BeautifulSoup(requests.get(url).content, 'html.parser')

    # Get the seo_url through class="JobSearchCard-primary-heading" html
    anchor_tag = soup.find_all('a', class_='JobSearchCard-primary-heading-link')

    # Get the 'href' attribute
    href_list = [tag['href'] for tag in anchor_tag]

    return href_list

#Read file json
def read_json_list_jobs(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        return json.load(file)
    
# From list of job in each page, get seo url of each job, and each contest
def get_seo_url_json_list_jobs(list_url):
    contests_seo_urls=[]
    projects_seo_urls=[]
    for url in list_url:
        
        seo_urls=get_job_atrributes(url)
        for seo_url in seo_urls:
            
            #If have 'contest' in url, append to contest list
            if seo_url[0:9]=='/contest/':
                contests_seo_urls.append(seo_url)
            else :
                projects_seo_urls.append(seo_url)
                
    #Remove "/projects/" in url
    projects_seo_urls=[url.replace("/projects/","") for url in projects_seo_urls]
    
    #Apply regex to get the contest id contain 7 digits
    contests_id=[re.search(r'\d{7}',url).group(0) for url in contests_seo_urls]
    
    return projects_seo_urls, contests_id

def create_api_url_request(p_base_url,p_params):
    
    # Encode parameters
    encoded_params = urllib.parse.urlencode(p_params, doseq=True)

    # Construct the full URL
    url = f'{p_base_url}?{encoded_params}'

    # Headers for authorization
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    return url, headers


# Get data from API, input is list of contest id, output is data response from API
def get_contests(p_contests_id,p_from_time=None,p_to_time=None):
    
    # Parameters contructed as a dictionary
    params = {
        'contests[]': p_contests_id,
        'job_details':True,
        'from_time': p_from_time,
        'to_time': p_to_time,
    }
    base_url = 'https://www.freelancer.com/api/contests/0.1/contests/'
    url, headers = create_api_url_request(base_url, params)
    try: 
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f'Error: {e}')
        return None

# Get data from API, input is list of project seo urls, output is data response from API
def get_projects(p_seo_urls,p_from_time=None,p_to_time=None):
    # Parameters contructed as a dictionary
    params = {
        'seo_urls[]': p_seo_urls,
        'full_description': True,
        'job_details': True,
        'from_time': p_from_time,
        'to_time': p_to_time,
    }
    base_url = 'https://www.freelancer.com/api/projects/0.1/projects/'
    url, headers = create_api_url_request(base_url, params)
    try: 
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f'Error: {e}')
        return None
def seo_url_too_long_handler(p_seo_urls,p_from_time=None,p_to_time=None):
    # Partition the list of URLs into smaller chunks
    chunk_size = 50
    chunks = [p_seo_urls[i:i + chunk_size] for i in range(0, len(p_seo_urls), chunk_size)]
    # Initialize an empty list to store the responses
    responses = []
    # Iterate over each chunk
    for chunk in chunks:
        # Get the response from the API
        response = get_projects(chunk, p_from_time, p_to_time)
        # Append the response to the list
        responses.append(response)
    return responses



# Get data from API, input is start_id, end_id, category (file list_jobs_URL_of_each_category), output is data response from API
def get_jobs(p_category,p_min,p_max,p_from_time=None,p_to_time=None):
    current_file_path=os.getcwd()
    #json.loads(open(, "r", encoding="utf-8").read())
    file_path="../../../data/raw/list_jobs_URL_of_each_category.json"
    body_content=read_json_list_jobs(file_path)


    # Get urls from p_min index to p_max index of category p_category
    body_content=body_content[str(p_category)][p_min:p_max]

    projects_seo_urls, contests_id=get_seo_url_json_list_jobs(body_content)

    projects=None
    contests=None
    if len(projects_seo_urls)>0:
        try:
            projects=seo_url_too_long_handler(projects_seo_urls,p_from_time,p_to_time)
        except:
            pass
    if len(contests_id)>0:
        try:
            contests=get_contests(contests_id, p_from_time, p_to_time)
        except:
            pass
    return projects, contests



# Get list of bider of project id from API
def get_bid(project_id):
    # Parameters contructed as a dictionary
    params = {

    }
    base_url = 'https://www.freelancer.com/api/projects/0.1/projects/{}/bids/'.format(project_id)
    url, headers = create_api_url_request(base_url, params)
    try: 
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f'Error: {e}')
        return None
# Get list of entrants of contest id from API (Competition)
def get_entrants(contest_ids:list):
    # Parameters contructed as a dictionary
    params = {
        'contest_ids[]': contest_ids,
    }
    base_url = 'https://www.freelancer.com/api/contests/0.1/entrants/all/'
    url, headers = create_api_url_request(base_url, params)
    try: 
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f'Error: {e}')
        return None
    
    
    
# Convert data from API to dataframe
def to_df_projects_contests(projects,contests,log_status=True):
    df=pd.DataFrame(columns=["job_id","owner_id","type","title","services","skills","description",
                             "date_posted","time_remaining","budget_min","budget_max","budget_currency",
                             "exchange_rate","bid_count","bid_list","average_bid","status","language","url"])
    #Save projects to dataframe
    try: 
        for projects_chunk in projects:
            for project in projects_chunk['result']['projects']:
                if project is None:
                    continue
                
                if log_status:
                    print("INFO: Getting project with id: ",project['id'])
                    
                job_id=project['id']
                owner_id=project['owner_id']
                _type="Project"
                title=project['title']
                services=project['seo_url'].split("/")[0]
                skills=[skill['name'] for skill in project['jobs']]
                description=project['description']
                date_posted=project['submitdate']
                time_free_bids_expire=project['time_free_bids_expire']
                budget_min=project['budget']['minimum']
                budget_max=project['budget']['maximum']
                budget_currency=project['currency']['code']
                exchange_rate=project['currency']['exchange_rate']
                bid_count=project['bid_stats']['bid_count']
                try:
                    bid_list=[bid_user['bidder_id'] for bid_user in get_bid(job_id)['result']['bids']]
                except:
                    bid_list=None
                average_bid=project['bid_stats']['bid_avg']
                status=project['status']
                language=project['language']
                url="https://www.freelancer.com/projects"+project['seo_url']
                #Check if the job_id already exists in the dataframe
                if job_id in df['job_id'].values:
                    continue
                else:
                    new_row_df=pd.DataFrame({"job_id":job_id,"owner_id":owner_id,"type":_type,"title":title,\
                                            "services":services,"skills":[skills],"description":description,\
                                            "date_posted":date_posted,"time_remaining":time_free_bids_expire,\
                                            "budget_min":budget_min,"budget_max":budget_max,"budget_currency":budget_currency,\
                                            "exchange_rate":exchange_rate,"bid_count":bid_count,"bid_list":[bid_list],"average_bid":average_bid,\
                                            "status":status,"language":language,"url":url})
                    df=pd.concat([df,new_row_df],ignore_index=True)
    except:
        pass
    
    
    #Save contests to dataframe
    try: 
        for contest in contests['result']['contests']:
            if contest is None:
                continue
            if log_status:
                print("INFO: Getting contest with id: ",contest['id'])
            job_id=contest['id']
            owner_id=contest['owner_id']
            _type="Contest"
            title=contest['title']
            services=None
            skills=[skill['name'] for skill in contest['jobs']]
            description=contest['description']
            date_posted=contest['time_submitted']
            time_remaining=None
            budget_min=contest['prize']
            budget_max=contest['prize']
            budget_currency=contest['currency']['code']
            exchange_rate=contest['currency']['exchange_rate']
            try:
                bid_list=[bid_user['id'] for bid_user in get_entrants([job_id])['result']['entrants']]
            except:
                bid_list=None
            bid_count=len(bid_list)
            average_bid=None
            status=contest['status']
            language=contest['language']
            url="https://www.freelancer.com/"+contest['seo_url']
            #Check if the job_id already exists in the dataframe
            if job_id in df['job_id'].values:
                continue
            else:
                new_row_df=pd.DataFrame({"job_id":job_id,"owner_id":owner_id,"type":_type,"title":title,\
                                        "services":services,"skills":[skills],"description":description,\
                                        "date_posted":date_posted,"time_remaining":time_remaining,\
                                        "budget_min":budget_min,"budget_max":budget_max,"budget_currency":budget_currency,\
                                        "exchange_rate":exchange_rate,"bid_count":bid_count,"bid_list":[bid_list],"average_bid":average_bid,\
                                        "status":status,"language":language,"url":url})
                df=pd.concat([df,new_row_df],ignore_index=True)
    except:
        pass
    return df


# Save dataframe to csv file (Projects and Contests)
def save_projects_contests_to_csv(df,category,p_min,p_max):
    current_file_path=os.getcwd()
    new_date=datetime.now().strftime("%d%m%Y")
    file_path="/".join((os.path.dirname(os.path.dirname(os.path.dirname(current_file_path))),\
                        "data","raw",f"freelancer_jobs_category_{category}_{p_min}_{p_max}_{new_date}.csv"))
    df.to_csv(file_path,index=False)
    return file_path


# Main function to get data projects and contests from API
def get_data_jobs(category,p_min,p_max,p_from_time,p_to_time,log_status=True):
    projects,contests=get_jobs(category,p_min,p_max,p_from_time,p_to_time)
    df=to_df_projects_contests(projects,contests,log_status)
    file_path=save_projects_contests_to_csv(df,category,p_min,p_max)
    return file_path

# Get list of user (bider and entrants) from csv file
def get_user_id(file_path):
    df=pd.read_csv(file_path)
    return df['bid_list'].values

# Get data from API, input is list of user id, output is data response from API
def get_user(p_user_ids):
     # Parameters contructed as a dictionary
    params = {
        'users[]': p_user_ids,
        'profile_description':True,
        'reputation'  :True,
        'reputation_extra':True,
    }
    base_url = 'https://www.freelancer.com/api/users/0.1/users/'
    url, headers = create_api_url_request(base_url, params)
    try: 
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f'Error: {e}')
        return None
    
    
# Convert data from API to dataframe
def to_df_users(data):
    df=pd.DataFrame(columns=["user_id","username","region","overview","ratings","completed_ratings","url"])
    for user in list(data['result']['users'].values()):
        user_id=user['id']
        username=user['username']
        region=user['location']['country']['name']
        overview=user['profile_description']
        ratings=user['reputation']['entire_history']['overall']
        completed_ratings=user['reputation']['entire_history']['completion_rate']
        url="https://www.freelancer.com/u/"+username
        if user_id in df['user_id'].values:
            continue
        else:
            new_row_df=pd.DataFrame({"user_id":user_id,"username":username,"region":region,\
                                    "overview":overview,"ratings":ratings,"completed_ratings":completed_ratings,\
                                    "url":url},index=[0])
        df=pd.concat([df,new_row_df],ignore_index=True)
    return df


# Save dataframe to csv file (Users)
def save_users_to_csv(df,category,p_min,p_max):
    current_file_path=os.getcwd()
    new_date=datetime.now().strftime("%d%m%Y")
    file_path="/".join((os.path.dirname(os.path.dirname(os.path.dirname(current_file_path))),\
                        "data","raw",f"freelancer_users_category_{category}_{p_min}_{p_max}_{new_date}.csv"))
    df.to_csv(file_path,index=False)
    return file_path

# Main function to get data users from API
def get_data_users(file_jobs_path,p_categories,p_min,p_max,log_status=True):
    list_user_ids=get_user_id(file_jobs_path)
    df=pd.DataFrame(columns=["user_id","username","region","overview","ratings","completed_ratings","url"])
    for i,user_ids in enumerate(list_user_ids):
        try:
            user_ids=eval(user_ids)
            if log_status:
                print("INFO: Getting user info with batch: ",i)
            data=get_user(user_ids)
            new_df=to_df_users(data)
            df=pd.concat([df,new_df],ignore_index=True)
        except:
            print('except')
            pass
        time.sleep(1)
    file_path=save_users_to_csv(df,p_categories,p_min,p_max)
    return file_path

def connect_to_db():
    load_dotenv()
    server=os.getenv('HOSTNAME')
    database=os.getenv('DATABASE')
    user=os.getenv('USERNAME')
    password=os.getenv('PASSWORD')
    try :
        conn = pymssql.connect(server, user, password, database)
    except:
        raise Exception("Connection failed!")
    return conn
def get_last_date_posed():
    conn=connect_to_db()
    cur=conn.cursor()
    cur.execute("SELECT MAX(date_posted) FROM jobs")
    last_date_posted=cur.fetchmany(1)[0][0]
    cur.close()
    conn.close()
    return last_date_posted

def get_from_time_to_crawl_new_data():
    last_date_posted=get_last_date_posed()
    last_date_str=last_date_posted.strftime("%d/%m/%Y")+ " 00:00:00"
    return int(datetime.strptime(last_date_str, "%d/%m/%Y %H:%M:%S").timestamp())

def main():
    parser = argparse.ArgumentParser(description="Example of optional arguments")
    parser.add_argument("--npage", type=int, help="Your n page in per category", default=50)
    parser.add_argument("--category", type=list, help="Your category", default=[2,3,6,7])
    parser.add_argument("--logs", type=bool, help="Logs status", default=True)
    args = parser.parse_args()
    
    #Get lastdate of data crawled from database 
    from_time=get_from_time_to_crawl_new_data()
    print(from_time)
    categories=args.category
    for category in categories:
        get_list_job_URL_of_each_category()
        file_path="/".join((os.path.dirname(os.path.dirname(os.path.dirname(os.getcwd()))),\
                            "data","raw","list_jobs_URL_of_each_category.json"))
        with open(file_path, 'r', encoding='utf-8') as file:
            body_content=json.load(file)
        n_jobs_page_link_jobs=len(body_content[str(category)])
    
        
        n_page=args.npage
        for i in range(0,n_jobs_page_link_jobs,n_page):
            file_path=get_data_jobs(category,i,i+n_page,p_from_time=from_time,p_to_time=None)
            get_data_users(file_path,category,i,i+n_page)
            
            
if __name__ == "__main__":
    main()