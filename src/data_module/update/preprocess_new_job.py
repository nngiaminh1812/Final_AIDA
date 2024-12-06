
import pandas as pd
import re
import os

# # Xử lí file job post
#đọc file new job posts 
df = pd.read_csv('new_raw_jobs.csv')
# print(df.head(10))

# Loại bỏ các secret project vì chúng không có thông tin gì cả
#if type = "Secret project" thì drop
df = df[df['Type'] != 'Secret project']

# Loại bỏ ký tự xuống dòng (\n) trong Title
df['Title'] = df['Title'].str.replace('\n', ' ')

# Do không cần dùng trạng thái công việc nên ta sẽ loại bỏ các giá trị "Đã hoàn thành", "Hết hạn nhận chào giá", "Mới
df['Title'] = df['Title'].str.replace('Đã hoàn thành|Hết hạn nhận chào giá|Mới', '', regex=True).str.strip()

#Loại bỏ \n và "Dịch vụ cần thuê" trong Services
df['Services'] = df['Services'].str.replace('\n|Dịch vụ cần thuê:', '', regex=True).str.strip()

# Loại bỏ ký tự xuống dòng (\n) và từ "Kỹ năng" trong Skills
df['Skills'] = df['Skills'].str.replace(r'\n|Kỹ năng', '', regex=True).str.strip()

# Loại bỏ khoảng trắng thừa trước dấu phẩy (nếu có)
# df['Skills'] = df['Skills'].str.replace(' ,', ',', regex=False).str.strip()
df['Skills'] = df['Skills'].str.replace(r'\s*,\s*', ',', regex=True)

#Đổi "\n" thành " " trong Description
df['Description'] = df['Description'].str.replace('\n', ' ').str.strip()

def extract_info(info):
    result = {
        'ID dự án': None,
        'Ngày đăng': None,
        'Chỉ còn': None,
        'Địa điểm': None,
        'Ngân sách': None,
        'Hình thức làm việc': None,
        'Hình thức trả lương': None
    }
    
    # Tạo các mẫu biểu thức chính quy cho từng mục
    patterns = {
        'ID dự án': r'ID dự án\n(\d+)|Id\n\s*(\d+)',
        'Ngày đăng': r'Ngày đăng\n\s+([^\n]+)',
        'Chỉ còn': r'Chỉ còn\n([^\n]+)',
        'Địa điểm': r'Địa điểm\n\s+([^\n]+)',
        'Ngân sách': r'Ngân sách\n\s+([^\n]+(?:\s+-\s+[^\n]+)?)|Tổng ngân sách\n\s+([^\n]+)',  # Cập nhật cho hai dạng Ngân sách
        'Hình thức làm việc': r'Hình thức làm việc\n\s+([^\n]+)',
        'Hình thức trả lương': r'Hình thức trả lương\n\s+([^\n]+)'
    }
    
    # Áp dụng từng mẫu để trích xuất giá trị
    for key, pattern in patterns.items():
        match = re.search(pattern, info)
        if key == 'ID dự án':
            if match is not None and match.group(1) is not None:
                result[key] = match.group(1).strip()
            elif match is not None and match.group(2) is not None:
                result[key] = match.group(2).strip()
            else:
                result[key] = None
        elif key == 'Ngân sách':
            if match is not None and match.group(1) is not None:
                result[key] = match.group(1).strip()
            elif match is not None and match.group(2) is not None:
                result[key] = match.group(2).strip()
            else:
                result[key] = None
        else:
            if match is not None and match.group(1) is not None:
                result[key] = match.group(1).strip()
            else:
                result[key] = None

    return result


# Giả sử df là DataFrame của bạn
info_list = []
# Thay thế các giá trị NaN bằng chuỗi rỗng
df["In4_project"] = df["In4_project"].fillna("")

for index, project_info in enumerate(df["In4_project"]):
    print(f"Processing project {index}...") 
    extracted_info = extract_info(project_info)
    info_list.append(extracted_info)


#nối info_list vào df
info_df = pd.DataFrame(info_list)

# Đặt lại chỉ số, loại bỏ chỉ số cũ
df.reset_index(drop=True, inplace=True)
#nối info_df vào df
df = pd.concat([df, info_df], axis=1)

# Xóa khoảng trắng dư thừa ở bất kỳ đâu trong chuỗi của cột "Ngân sách"
df["Ngân sách"] = df["Ngân sách"].str.replace(r'\s+', ' ', regex=True).str.strip()

#drop các dòng mà cột ID dự án = null
df = df.dropna(subset=["ID dự án"]) 
#reset index
df.reset_index(drop=True, inplace=True)

#'\n                Chào giá:                                        \n                        1                    \n'
df["Num_applicants"] = df["Num_applicants"].str.replace(r'\n|\s+|Chào giá:', '', regex=True).str.strip()

#Bỏ dấu "."
df["Price"] = df["Price"].str.replace('.', '')


# Sử dụng regex để tách giá trị thấp nhất, trung bình và cao nhất
df['Chào giá thấp nhất'] = df['Price'].str.extract(r'Thấp nhất:\s*([\d]+)').astype(float)
df['Chào giá trung bình'] = df['Price'].str.extract(r'Trung bình:\s*([\d]+)').astype(float)
df['Chào giá cao nhất'] = df['Price'].str.extract(r'Cao nhất:\s*([\d]+)').astype(float)

#đổi tên cột Duration thành Duration (ngày)
df.rename(columns={"Duration": "Duration (ngày)"}, inplace=True)
#chỉ giữ số trong cột Duration (ngày)
df["Duration (ngày)"] = df["Duration (ngày)"].str.replace(r'\n|\s+|Trung bình:|ngày|-', '', regex=True).str.strip()

# Hàm để tách ngân sách thấp nhất và cao nhất
def parse_budget(budget):
    # Xóa ký tự 'đ' và chuyển đổi thành số nguyên
    budget = budget.replace('đ', '').replace('.', '').strip()
    
    # Kiểm tra có dấu '-' không
    if '-' in budget:
        low, high = budget.split('-')
        return (float(low.strip()), float(high.strip()))
    else:
        # Không có dấu '-', gán ngân sách thấp nhất là 0
        return (0.0, float(budget))

# Áp dụng hàm để tách ngân sách
df[['Ngân sách thấp nhất', 'Ngân sách cao nhất']] = df['Ngân sách'].apply(parse_budget).apply(pd.Series)

#Cỉ lấy các cột cần thiết: ID dự án, Type, Title, Services, Skills, Description, Ngày đăng , Chỉ còn , Địa điểm ,Ngân sách thấp nhất, Ngân sách cao nhất, Hình thức làm việc, Hình thức trả lương, Num_applicants, Chào giá thấp nhất, Chào giá trung bình, Chào giá cao nhất, Duration (ngày)
df = df[['ID dự án', 'Type', 'Title', 'Services', 'Skills', 'Description', 'Ngày đăng', 'Chỉ còn', 'Địa điểm', 'Ngân sách thấp nhất', 'Ngân sách cao nhất', 'Hình thức làm việc', 'Hình thức trả lương', 'Num_applicants', 'Chào giá thấp nhất', 'Chào giá trung bình', 'Chào giá cao nhất', 'Duration (ngày)', "Link"]]

#Tính tỉ lệ null của các cột
print(df.isnull().mean() * 100)


#fill null bằng giá trị trung bình Chào giá thấp nhất và Chào giá cao nhất/2
df["Chào giá trung bình"].fillna((df["Chào giá thấp nhất"] + df["Chào giá cao nhất"]) / 2, inplace=True)

#Điền Service bằng mode 
df["Services"].fillna(df["Services"].mode()[0], inplace=True)

#điền Skills bằng mode
df["Skills"].fillna(df["Skills"].mode()[0], inplace=True)

# Tạo từ điển để ánh xạ tên cột từ tiếng Việt sang tiếng Anh
column_mapping = {
    'ID dự án': 'Project ID',
    'Ngày đăng': 'Posted Date',
    'Chỉ còn': 'Remaining Days',
    'Địa điểm': 'Location',
    'Ngân sách thấp nhất': 'Minimum Budget',
    'Ngân sách cao nhất': 'Maximum Budget',
    'Hình thức làm việc': 'Working Type',
    'Hình thức trả lương': 'Payment Type',
    'Chào giá thấp nhất': 'Lowest Bid',
    'Chào giá trung bình': 'Average Bid',
    'Chào giá cao nhất': 'Highest Bid',
    'Duration (ngày)': 'Duration (Days)',
}

# Áp dụng ánh xạ để đổi tên cột
df = df.rename(columns=column_mapping)
#ghi xuống file csv ..\..\..\..\..\data\processed\cleaned_job_posts.csv
df.to_csv('new_cleaned_jobs.csv', index=False)

