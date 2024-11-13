import pandas as pd
import numpy as np
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time
# import requests
import re
import sqlite3

# log in into account 
driver = webdriver.Chrome()
driver.get("https://www.vlance.vn/#")

login_button = driver.find_element(By.ID, "btn-login")
login_button.click()
time.sleep(5)

username = driver.find_element(By.ID, "login_username_header")
password = driver.find_element(By.ID, "login_password_header")
username.send_keys("misamotyrose@gmail.com")
password.send_keys("123456asd*")

print('Solve captcha manually...............')
time.sleep(30)

sumbit_button = driver.find_element(By.ID, "btn-submit-login-header")
sumbit_button.click()
# end of log in process

columns = [
    "Type", "Title", "Services", "Skills", "Description", 
    "In4_project", "In4_employment", "Num_applicants", 
    "Applicants", "Duration", "Price", "Link"
]

df = pd.DataFrame(columns=columns)
db_file = "vlance_db.sqlite"

# connect to sqlite db to get ID job posts from job_posts table
conn = sqlite3.connect(db_file)
query = 'SELECT "Project ID" FROM job_posts ORDER BY "Project ID" DESC LIMIT 1' # compare with 10 latest job id
cursor = conn.cursor()
cursor.execute(query)
id_latest = cursor.fetchone()
conn.close()
print(f"The latest job with ID :{id_latest[0]}")


page = 1
flag = False
while True:

    driver.get(f"https://www.vlance.vn/viec-lam-freelance/cpath_cac-cong-viec-it-va-lap-trinh_page_{page}")
    print(f"Page: {page}")
    try:
        WebDriverWait(driver, 23).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, "fr-name"))
        )
    except Exception as e:
        print("Error: ", e)
        driver.quit()
    
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    h3_tags = soup.find_all('h3', attrs={'class':"fr-name block-title"})
    print(len(h3_tags))

    for h3 in h3_tags:
        type = np.nan 
        service = np.nan
        skills = np.nan
        in4_project = np.nan
        in4_employment = np.nan
        num_applicants = np.nan
        budget = np.nan
        duration_avg = np.nan
        applicants = np.nan
        list_applicants = []
        bid_counter = np.nan
        bid_price = np.nan
        title = np.nan
        description = np.nan
        url = np.nan

        print(h3.find('a')['href'])
        url = f"https://www.vlance.vn{h3.find('a')['href']}"
    
        driver.get(url)
        try:
            WebDriverWait(driver, 30).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "h1.title.block-title"))
            )
        except Exception as e:
            print("Error: ", e)
            driver.quit()

        
        time.sleep(30)

        soup = BeautifulSoup(driver.page_source, 'html.parser')

        # Hanlde case secrect project
        secret = soup.find('div', attrs={'class':"span12 btn-connect"})
        if secret:
            print("Secret project")
            continue

        # case normal project
        type = soup.find('span', attrs={'class':"job-progress-title"})
        type_acp_bid = soup.find('div', attrs={'class':"progress-job"})
       

        if type and type.text == "Chiến dịch hiển thị":
            print('Chiến dịch hiển thị')
            title = soup.find('h1', attrs={'class':"title block-title"}).text

            # service = soup.find('div', attrs={'class':"service-title"}).text
            service = np.nan

            description = soup.find('div', attrs={'class':"row-fluid body-view review-text"}).text

            # skills = soup.find('div', attrs={'class':"span10 skill"}).text
            skills = np.nan

            in4_project = soup.find('div', attrs={'class':"description-job"})
            if in4_project:
                in4_project = in4_project.text
            else:
                in4_project = np.nan

            in4_employment = soup.find('div', attrs={'class':"info-employment"})
            if in4_employment:
                in4_employment = in4_employment.text
            else:
                in4_employment = np.nan


            num_applicants = soup.find('span', attrs={'class':"bid-samples"}).text

            budget = soup.find('div', attrs={'class':"span4 client-bidding-between"}).text

            duration_avg = soup.find('span', attrs={'class':"duration-average"}).text

            applicants = np.nan
            
        else:
            if type and type.text == "Đăng việc":
                print('Đăng việc')
                title = soup.find('h1', attrs={'class':"title block-title"}).text

                service = soup.find('div', attrs={'class':"service-title"})
                if service:
                    service = service.text
                else: 
                    service = np.nan

                description = soup.find('div', attrs={'class':"span10 description"}).text

                skills = soup.find('div', attrs={'class':"span10 skill"})
                if skills:
                    skills = skills.text
                else:
                    skills = np.nan

                in4_project = soup.find('div', attrs={'class':"description-job"})
                if in4_project:
                    in4_project = in4_project.text
                else:
                    in4_project = np.nan

                in4_employment = soup.find('div', attrs={'class':"info-employment"})
                if in4_employment:
                    in4_employment = in4_employment.text
                else:
                    in4_employment = np.nan

                bid_counter = soup.find('span', attrs={'class':"bid-counter"}).text

                bid_price = soup.find('div', attrs={'class':"span7 offset1 client-bidding-between-new"}).text

                duration_avg = soup.find('span', attrs={'class':"duration-average"}).text

                applicants = soup.find('div', attrs={'class':"row-fluid container list-bid-new"})
                list_applicants = []
                if applicants:
                    a_tags = applicants.find_all('h3', attrs={'class':"title"})
                    for user_href in a_tags:
                        list_applicants.append(user_href.find('a')['href'])

            else:
                if type_acp_bid and re.search(r"Nhận chào giá", type_acp_bid.text.strip()):
                    print("Nhan chao gia")
                    title = soup.find('h1', attrs={'class':"title block-title"}).text

                    service = soup.find('div', attrs={'class':"service-title"})
                    if service:
                        service = service.text
                    else: 
                        service = np.nan

                    description = soup.find('div', attrs={'class':"span10 description"}).text

                    skills = soup.find('div', attrs={'class':"span10 skill"})
                    if skills:
                        skills = skills.text
                    else:
                        skills = np.nan

                    in4_project = soup.find('div', attrs={'class':"description-job"})
                    if in4_project:
                        in4_project = in4_project.text
                    else:
                        in4_project = np.nan

                    in4_employment = soup.find('div', attrs={'class':"info-employment"})
                    if in4_employment:
                        in4_employment = in4_employment.text
                    else:
                        in4_employment = np.nan

                    bid_counter = soup.find('span', attrs={'class':"bid-counter"}).text

                    bid_price = soup.find('div', attrs={'class':"span6 client-bidding-between"}).text

                    duration_avg = soup.find('span', attrs={'class':"duration-average"}).text

                    applicants = soup.find_all('div', attrs={'class':"profile-job-left-bottom"})
                    list_applicants = []
                    if applicants:
                        for user_href in applicants:
                            list_applicants.append(user_href.find('a')['href'])
            
                else:
                    print("So sick and tired")
                    continue
                    
        if in4_project:
            ID = re.search(r'ID dự án\n(\d+)|Id\n\s*(\d+)', in4_project.strip())
            if ID:
                ID = ID.group(1).strip() if ID.group(1) else ID.group(2).strip()

                if ID <= id_latest[0]: # if ID is duplicated or smaller than ids in table of db, then break
                    print("Crawled ID is smaller than or equal the latest existed one")
                    flag = True
                    break
                else:
                    if type and type.text == "Chiến dịch hiển thị":
                        new_sample = pd.DataFrame([{
                            "Type": type.text,
                            "Title": title,
                            "Services": service,
                            "Skills": skills,
                            "Description": description,
                            "In4_project": in4_project,
                            "In4_employment": in4_employment,
                            "Num_applicants": num_applicants,
                            "Applicants": applicants,
                            "Duration": duration_avg,
                            "Price": budget,
                            "Link": url
                        }])
                    
                    elif type and type.text == "Đăng việc":
                        new_sample = pd.DataFrame([{
                            "Type": type.text,
                            "Title": title,
                            "Services": service,
                            "Skills": skills,
                            "Description": description,
                            "In4_project": in4_project,
                            "In4_employment": in4_employment,
                            "Num_applicants": bid_counter,
                            "Applicants": list_applicants,
                            "Duration": duration_avg,
                            "Price": bid_price,
                            "Link": url
                        }])
                    else:
                        new_sample = pd.DataFrame([{
                            "Type": type.text,
                            "Title": title,
                            "Services": service,
                            "Skills": skills,
                            "Description": description,
                            "In4_project": in4_project,
                            "In4_employment": in4_employment,
                            "Num_applicants": bid_counter,
                            "Applicants": list_applicants,
                            "Duration": duration_avg,
                            "Price": bid_price,
                            "Link": url
                        }])
                    df = pd.concat([df, new_sample], ignore_index=True)
                
    if flag:
        print("There is no more updated job posts")
        break

    time.sleep(2)
    page += 1

df.to_csv("new_raw_jobs.csv", index=False)
    
