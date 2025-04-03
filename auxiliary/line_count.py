import os

def count_lines_in_file(filepath):
    """计算单个文件中的行数"""
    with open(filepath, 'r', encoding='utf-8') as file:
        return len(file.readlines())

def count_lines_in_directory(directory, extensions):
    """递归地计算指定目录及其子目录下的所有文件的行数"""
    total_lines = 0
    for root, dirs, files in os.walk(directory):
        for file in files:
            if any(file.endswith(ext) for ext in extensions):
                filepath = os.path.join(root, file)
                total_lines += count_lines_in_file(filepath)
    return total_lines

if __name__ == "__main__":
    directory = r'E:\BaiduSyncdisk\陈丽华老师项目\+集成电路供应链网络研究\code\KG_GenSystem'  # 项目根目录
    extensions = ['.py']  # 可以添加更多扩展名，如['.py', '.java', '.js']
    total_lines = count_lines_in_directory(directory, extensions)
    print(f"Total lines of code: {total_lines}")