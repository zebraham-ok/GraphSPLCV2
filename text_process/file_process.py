import re
import json
import os
import time

def sanitize_filename(filename):
    "去除文件名中所有不能作为文件名的字符串"
    new_name=re.sub(r'[<>:"/\\|?*\0]', '', filename)
    if len(new_name)>120:
        new_name=new_name[:120]
    return new_name

def save_json(response_dict,save_dir,name): 
    "保存json数据"
    response_text=json.dumps(response_dict,ensure_ascii=False)
    file_name=sanitize_filename(name)  # 确保文件名可用
    with open(os.path.join(save_dir,file_name),"w+",encoding="utf8") as file:
        file.write(response_text)


def file_freshness(file,display=False):
    "获取文件的修改时间（时间戳）距离现在多少天"
    mod_time = os.path.getmtime(file)
    freshness=(time.time()-mod_time)/3600/24  # 文件在多少天之前修改（非整数）
    # 将时间戳转换为更易读的格式
    if display:
        formatted_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(mod_time))
        print("文件最后一次修改的时间是:", formatted_time)
    return freshness