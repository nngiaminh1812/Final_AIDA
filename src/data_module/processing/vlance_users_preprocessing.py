import pandas as pd
import re
from datetime import date
import argparse

# Hàm để trích xuất thông tin
def extract_summary_info(summary):
    # Nếu freelancer chưa có đánh giá
    if "Freelancer chưa có đánh giá" in summary:
        return pd.Series([0, 0, 0, 0], index=["rating", "review_count", "completion_rate", "rehire_rate"])
    
    # Sử dụng regex để trích xuất thông tin
    match = re.search(r'(\d+,\d+)\s+(\d+).*?Hoàn thành việc\n\n(\d+)%.*?Được thuê lại\n\n(\d+)%', summary, re.DOTALL)
    
    if match:
        rating = float(match.group(1).replace(',', '.'))  # Chuyển đổi dấu phẩy thành dấu chấm cho đúng định dạng
        review_count = int(match.group(2))
        completion_rate = int(match.group(3))
        rehire_rate = int(match.group(4))
    else:
        # Nếu không khớp regex, trả về giá trị 0
        rating, review_count, completion_rate, rehire_rate = None, None, None, None

    return pd.Series([rating, review_count, completion_rate, rehire_rate], index=["rating", "review_count", "completion_rate", "rehire_rate"])

# Hàm để xác định số năm kinh nghiệm
def extract_experience(summary):
    if pd.isna(summary):  # Kiểm tra nếu là NaN
        return None
    
    if "Mới đi làm (Dưới 2 năm kinh nghiệm)" in summary:
        return "Mới đi làm (Dưới 2 năm kinh nghiệm)"
    elif "Đã có kinh nghiệm (2-5 năm kinh nghiệm)" in summary:
        return "Đã có kinh nghiệm (2-5 năm kinh nghiệm)"
    elif "Chuyên gia (trên 5 năm kinh nghiệm)" in summary:
        return "Chuyên gia (trên 5 năm kinh nghiệm)"
    else:
        return None  # Trả về NaN nếu không tìm thấy

def user_vlance_preprocess(input_path, user_output_path, application_output_path):

    # Load the data
    df = pd.read_csv(input_path, encoding='utf-8')

    #Voeis cột ID và id_project, chỉ giữ lại các con số, loại bỏ các ký tự khác
    df['id'] = df['id'].str.extract('(\d+)')
    df['id_project'] = df['id_project'].str.extract('(\d+)')

    #title và region loại bỏ các khoảng trắng ở đầu và cuối chuỗi, \n 
    df["title"] = df["title"].str.strip()
    df["region"] = df["region"].str.strip()
    df["overview"] = df["overview"].replace(r'Giới thiệu|\n|\s+', ' ', regex=True).str.strip()

    df["services"] = df["services"].str.replace('\n\n\n\n', '; ').replace(r'\n|\s+|Dịch vụ|Danh sách các dịch vụ được freelancer cung cấp:', ' ', regex=True).str.strip()
    # Áp dụng hàm trên cột "summary_profile"
    df[["rating", "review_count", "completion_rate", "rehire_rate"]] = df["summary_profile"].apply(extract_summary_info)

    # Áp dụng hàm để tạo cột "experience"
    df["experience"] = df["summary_working"].apply(extract_experience)

    #Lấy các cột cần dùng id, id_project, title, region, overview, service, rating, review_count, completion_rate, rehire_rate, experience
    new_df = df[["id", "id_project", "title", "region", "overview", "services", "rating", "review_count", "completion_rate", "rehire_rate", "experience"]]

    #kiểm tra trùng lặp: các dòng có gióng nhau cả id và id_project
    df_applications = df[["id", "id_project"]]
    df_applications.duplicated().sum()
    #Loại bỏ trùng
    df_applications = df_applications.drop_duplicates()
    df_applications.reset_index()
    # df_applications

    df_applications.to_csv(application_output_path, index=False)

    df_applicants = new_df.drop(columns=["id_project"])
    #kiểm tra trùng
    df_applicants.duplicated().sum()
    #loại bỏ trùng lặp
    df_applicants = df_applicants.drop_duplicates()

    #Tìm các freelancer có rating khác 0
    df_applicants[df_applicants["review_count"] != 0]

    #điền "Null" cho title, overview, services, experience
    df_applicants["title"] = df_applicants["title"].fillna("Null")
    df_applicants["overview"] = df_applicants["overview"].fillna("Null")

    #services và experience có thể điền mode
    df_applicants["services"] = df_applicants["services"].fillna(df_applicants["services"].mode()[0])

    #điền các dòng có rating, review_count, completion_rate, rehire_rate null là 0
    df_applicants["rating"] = df_applicants["rating"].fillna(0)
    df_applicants["review_count"] = df_applicants["review_count"].fillna(0)
    df_applicants["completion_rate"] = df_applicants["completion_rate"].fillna(0)
    df_applicants["rehire_rate"] = df_applicants["rehire_rate"].fillna(0)

    df_applicants.to_csv(user_output_path, index=False)
def main():
    parser = argparse.ArgumentParser(description="Example of optional arguments")
    parser.add_argument("--post_prefix", type=str, help="Your post prefix file name", default=date.today().strftime("%Y-%m-%d"))
    args = parser.parse_args()
    input_path = "../../../data/raw/vlance_users_{}.csv".format(args.post_prefix)
    user_output_path = "../../../data/processed/vlance_users_{}.csv".format(args.post_prefix)
    application_output_path = "../../../data/processed/vlance_applicants_{}.csv".format(args.post_prefix)
    user_vlance_preprocess(input_path, user_output_path, application_output_path)
if __name__ == "__main__":
    main()
