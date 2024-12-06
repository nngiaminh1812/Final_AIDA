# Use a pipeline as a high-level helper
#from transformers import pipeline
#import torch
import pandas as pd
#from transformers import AutoTokenizer, MarianMTModel
#from transformers import AutoModelForSeq2SeqLM
#device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
import pandas as pd
import json 
import re
import argparse
from datetime import date

def Convert_Type(x):
    if x=="Đăng việc":
        return "JobsPosting"
    elif x=="Nhận chào giá":
        return "Project"
    elif x=="Chiến dịch hiển thị":
        return "PresentedCampaign"
    else:
        return x
def Convert_Remaining_Days(x):
    if x == 'Hết hạn nhận chào giá' or x == 'Đã hoàn thành':
        return 0
    # is digit
    elif re.match(r'^\d+$', x.split('ngày')[0].strip()):
        return int(x.split('ngày')[0].strip())
    else :
        return 0
def Convert_Location(x):
    province_mapping=json.load(open("../../../data/utils/vietnam_province.json"))
    if x =='Toàn Quốc' :return 'Viet Nam'
    else :
        return province_mapping[x] if x in province_mapping else x
def Convert_WorkingType(x):
    mapping_working_type={
        "Làm online": "Online",
        "Làm tại văn phòng": "Onsite",
        "Làm online (đến văn phòng khi cần)":"Hybrid"}
    return mapping_working_type[x] if x in mapping_working_type else x
def Convert_PaymentType(x):
    mapping_payment_type={
        "Trả theo dự án":"Project-based",
        "Trả theo giờ":"Hourly",
        "Trả theo tháng":"Monthly",
    }
    return mapping_payment_type[x] if x in mapping_payment_type else x
def Convert_Region(x):
    province_mapping=json.load(open("../../../data/utils/vietnam_province.json"))
    if x =='Toàn Quốc' :return 'Viet Nam'
    else :
        return province_mapping[x] if x in province_mapping else x
def Convert_Experience(x):
    if x=="Mới đi làm (Dưới 2 năm kinh nghiệm)": 
        return "Fresher"
    elif x=="Đã có kinh nghiệm (2-5 năm kinh nghiệm)":
        return "Junior"
    elif x=="Chuyên gia (trên 5 năm kinh nghiệm)":
        return "Expert"
    else : return x
def translate_without_model(jobs_df,users_df):
    # Jobs
    jobs_df['Type']=jobs_df['Type'].apply(Convert_Type)
    jobs_df['Remaining Days']=jobs_df['Remaining Days'].apply(Convert_Remaining_Days)
    jobs_df['Location']=jobs_df['Location'].apply(Convert_Location)
    jobs_df['Working Type']=jobs_df['Working Type'].apply(Convert_WorkingType)
    jobs_df['Payment Type']=jobs_df['Payment Type'].apply(Convert_PaymentType)

    # Users
    users_df['region']=users_df['region'].apply(Convert_Region)
    users_df['experience']=users_df['experience'].apply(Convert_Experience)
    users_df=users_df.drop(columns=['title'])
    
    return jobs_df,users_df

def translate_with_mode(jobs_df,users_df):
    
    model_name = "facebook/m2m100_418M"
    model = AutoModelForSeq2SeqLM.from_pretrained(model_name).to(device)
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    tokenizer.src_lang = "vi"


    inputs = tokenizer("'Yêu cầu:  Làm giống Design thêm Responsive trên Mobile - Hiện tại cũng gần ổn rồi https://drive.google.com/drive/folders/1XE0uoc61uX30ED-4CkRhT4cqvJvj76UV?usp=sharing  Site đang làm dở: http://wondermedia.hostingviet.com.vn/  Kiểm tra xem còn chức năng nào chưa hoàn thiện thì xử lý nốt.  VD Trang chi tiết DV:  - Chưa đặt hàng được.  Thời gian hoàn thành: 4 ngày kể từ khi bắt đầu (tính cả T7, CN)'", return_tensors="pt").to(device)
    with torch.no_grad():
        translated_tokens = model.generate(**inputs, forced_bos_token_id=tokenizer.lang_code_to_id["en"])
        translation = tokenizer.batch_decode(translated_tokens, skip_special_tokens=True)
    print(translation)
    
def main():
    parser = argparse.ArgumentParser(description="Example of optional arguments")
    parser.add_argument("--post_prefix", type=str, help="Your post prefix file name", default=date.today().strftime("%Y%m%d"))
    args = parser.parse_args() 
    jobs_df = pd.read_csv("../../../data/processed/vlance_jobs_{}.csv".format(args.post_prefix))
    users_df = pd.read_csv("../../../data/processed/vlance_users_{}.csv".format(args.post_prefix))
    jobs_df,users_df=translate_without_model(jobs_df,users_df)
    #translate_with_model(jobs_df,users_df)
    users_df.to_csv("../../../data/processed/vlance_users_{}.csv".format(args.post_prefix),index=False)
    jobs_df.to_csv("../../../data/processed/vlance_jobs_{}.csv".format(args.post_prefix),index=False)
if __name__ == "__main__":
    main()
    