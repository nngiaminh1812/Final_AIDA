import re


# Hàm trích xuất Experience
def extract_experience(text):
    # Sử dụng regex để tìm nội dung dưới phần "Experience"
    experience_pattern = r"Experience\n(.*?)\nEducation"
    experience = re.search(experience_pattern, text, re.DOTALL)
    if experience:
        return experience.group(1).strip()
    else:
        return "Experience not found"
    
def extract_summary(text):
    # Sử dụng regex để tìm nội dung dưới phần "Summary"
    summary_pattern = r"\n(.*?)\nExperience"
    summary = re.search(summary_pattern, text, re.DOTALL)
    if summary:
        return summary.group(1).strip()
    else:
        return "Summary not found"
    
# Hàm trích xuất Skills
def extract_skills(text):
    # Sử dụng regex để tìm nội dung dưới phần "Skills"
    skills_pattern = r"Skills\n(.*?)\nCertification"
    skills = re.search(skills_pattern, text, re.DOTALL)
    if skills:
        # Chuyển danh sách các kỹ năng thành chuỗi, cách nhau bởi dấu phẩy
        skill_list = skills.group(1).strip().split()
        return ", ".join(skill_list)  # Kết hợp danh sách thành chuỗi
    else:
        return "Skills not found"