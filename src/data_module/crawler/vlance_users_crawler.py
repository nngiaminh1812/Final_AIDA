import pandas as pd
import numpy as np
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time
import requests
import re
from datetime import date
from dotenv import load_dotenv
import os 


def load_env_config():
    # Load environment variables from .env file
    try:
        load_dotenv()
        username_key = os.getenv("GMAIL")
        password_key = os.getenv("GMAIL_PASSWORD")
        if username_key is None or password_key is None:
            raise Exception("No access token found.")
        else: 
            print("Environment variables loaded successfully.")
            return username_key, password_key
    except:
        raise Exception("No .env file found.")

def user_update():
    # Load environment variables
    username_key, password_key = load_env_config()
    driver = webdriver.Chrome()
    driver.get("https://www.vlance.vn/#")

    login_button = driver.find_element(By.ID, "btn-login")
    login_button.click()
    time.sleep(5)

    username = driver.find_element(By.ID, "login_username_header")
    password = driver.find_element(By.ID, "login_password_header")
    username.send_keys(username_key)
    password.send_keys(password_key)

    print('Solve captcha manually...............')
    time.sleep(30)

    sumbit_button = driver.find_element(By.ID, "btn-submit-login-header")
    sumbit_button.click()

    df = pd.read_csv("../../../data/raw/vlance_jobs_{}.csv".format(date.today().strftime("%Y-%m-%d")), encoding='utf-8')

    # Ensure that the Applicants column is a list-like structure
    df = df.dropna(subset=['Applicants'])
    df['Applicants'] = df['Applicants'].apply(lambda x: x.replace('[', '').replace(']', ''))
    df['Applicants'] = df['Applicants'].apply(lambda x: x.split(','))

    # Apply the explode method only if the values are lists
    applicants = df.explode('Applicants')
    # applicants.head()

    applicants['Applicants'] = applicants['Applicants'].apply(lambda x: x.replace("'", '') if isinstance(x, str) else x)
    applicants = applicants.reset_index(drop=True)

    df = pd.DataFrame(columns=['id', 'id_project', 'title', 'region', 'overview', 'services', 'summary_profile', 'summary_working', 'link'])
    ID_user = []
    ID_project = []
    Title = []
    Skills = []
    Region = []
    Overview = []
    Services = []
    Summary_profile = []
    Summary_working = []
    Link = []

    for i in range(0, len(applicants)):
        url = np.nan
        id_project = np.nan
        id_user = np.nan
        title = np.nan
        skills = np.nan
        region = np.nan
        overview = np.nan
        services = np.nan
        summary_profile = np.nan
        summary_working = np.nan

        print(f"Processing freelancer {i}/{len(applicants)}")
        if applicants["Applicants"][i] == '':
            continue

        url = "https://www.vlance.vn"+ applicants['Applicants'][i].strip()
        print(url)

        driver.get(url)
            # Kiểm tra HTTP status trước khi tải trang
        try:
            h1_heading = driver.find_element(By.TAG_NAME, "h1").text
            print(h1_heading)
            if h1_heading == "LỖI 404":
                print(f"URL {url} hiển thị 'LỖI 404'. Bỏ qua...")
                continue
        except Exception as e:
            print(f"Không tìm thấy thẻ h1 hoặc xảy ra lỗi khác: {e}")

        Link.append(url)

        id_project = re.search(r'ID dự án\n(\d+)|Id\n\s*(\d+)', applicants['In4_project'][i].strip())
        ID_project.append(id_project.group(1).strip() if id_project.group(1) else id_project.group(2).strip())

        try:
            WebDriverWait(driver, 30).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.id_profile"))
            )
        except Exception as e:
            print("Error: ", e)
            driver.quit()
            
        time.sleep(10)
        try:
            see_more_skill = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "see-more-skill"))
            )
            see_more_skill.click()
            print("Clicked on 'see-more-skill'")
        except Exception as e:
            print("Element 'see-more-skill' not found or not clickable:")

        
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        id_user = soup.find('div', attrs={'class':"id_profile"}).text
        ID_user.append(id_user)

        # name = soup.find('span', attrs={'itemprop':"name"}).text
        title = soup.find('span', attrs={'itemprop':"title editable tf300"})
        if title:
            Title.append(title.text)
        else:
            Title.append(np.nan)

        skills = soup.find('div', attrs={'class':"skills"})
        if skills:
            Skills.append(skills.text)
        else:
            Skills.append(np.nan)

        region = soup.find('div', attrs={'class':"regional-price"})
        if region:
            Region.append(region.text)
        else:
            Region.append(np.nan)

        overview = soup.find('div', attrs={'class':"overview overview-mobile"})
        if overview:
            Overview.append(overview.text)
        else:
            Overview.append(np.nan)

        services = soup.find('div', attrs={'class':"service-freelancer"})
        if services:
            Services.append(services.text)
        else:
            Services.append(np.nan)

        
        summary_profile = soup.find('div', attrs={'class':"summary-profile profile-rate summary-title summary-block"})
        if summary_profile:
            Summary_profile.append(summary_profile.text)
        else:
            Summary_profile.append(np.nan)
            
        summary_working = soup.find('div', attrs={'class':"summary-profile profile-rate summary-working"})
        if summary_working:
            Summary_working.append(summary_working.text)
        else:
            Summary_working.append(np.nan)

    df =  pd.DataFrame({'id': ID_user, 'id_project': ID_project, 'title': Title, 'region': Region, 
                        'overview': Overview, 'services': Services, 'summary_profile': Summary_profile, 
                        'summary_working': Summary_working, 'link': Link})

    df.to_csv("vlance_users_{}.csv".format(date.today().strftime("%Y-%m-%d")), index = False)

    driver.quit()

if __name__ == '__main__':
    user_update()