"""存储了多种封装好的大语言模型
对多重括号的处理也放在了这里
"""

from http import HTTPStatus
import re
import json
# from API.neo4j_SLPC import *
# from spider.web_search import *
from .secret_manager import read_secrets_from_csv
import requests
import os
import openai


current_file = os.path.abspath(__file__)
current_dir = os.path.dirname(current_file)
secret_dict=read_secrets_from_csv(filename=os.path.join(current_dir,"secrets.csv"))
# print(secret_dict)
openai_key=secret_dict["openai"]
# gemini_key=secret_dict["gemini_cost1"]
# gemini_free_key=secret_dict["gemini_free2"]
general_key=secret_dict["general"]
# metaso_key=secret_dict["metaso_key"]

def find_outer_braces(s): 
    "获取一个字符串中所有的最外层大括号（避免因为没生成完导致缺少反括号]的错误），并直接将他们转化为字典，以list[dict]形式返回"
    stack = []
    result = []
    start = None

    for i, char in enumerate(s):
        if char == '{':
            if not stack:
                start = i  # 记录最外层 '{' 的位置
            stack.append(i)
        elif char == '}':
            if stack:
                stack.pop()
                if not stack:
                    result.append(s[start:i+1])  # 记录最外层括号的内容
                    
    list_of_dict=[]
    for i in result:
        if isinstance(i,str):
            try:
                list_of_dict.append(json.loads(i))
            except Exception as e:
                print(f"cannot conver {i} to json because {e}")
    return list_of_dict

def solve_nested_quotes(text):
    "这段代码可以解决嵌套双引号导致无法将gemini生成的字符串转化为字典的问题"
    # 匹配嵌套双引号的部分
    match = re.search(r'(?<=: ")([^"]*\"[^"]*\"[^"]*)(?=",\n|"\n|", \n)', text)
    # print(match)
    if match:
        sub_matched_str = re.sub(r'\"', "'", match.group())
        new_text = text.replace(match.group(), sub_matched_str)
        return new_text
    else:
        return text

def process_json_response(outer_braces_list):
    "专门用来处理[str(dict), str(dict)...]形式的数据，将其变为真正的字典"
    parsed_data_list=[]
    for outer_brace in outer_braces_list:
        temp_parsed_dict={}
        try:
            temp_parsed_dict=eval(outer_brace.replace("null","None"))
        except Exception as e:  # eval是用Python运行程序，因此无法处理json中的null，而json.loads可以
            print(f"eval function failed with {e} trying solve nested quotes")
        if temp_parsed_dict and isinstance(temp_parsed_dict,dict):
            parsed_data_list.append(temp_parsed_dict)
            continue
        try:   # 在尝试将字符串转化为字典的时候，要注意文本中可能存在的反斜杠(replace解决子，re可以)
            outer_brace=solve_nested_quotes(outer_brace)
            outer_brace=outer_brace.replace("\n","").replace("  ","")  # 避免多余部分
            outer_brace=re.sub('''",}''','''"}''',outer_brace)
            outer_brace=re.sub('''", }''','''"}''',outer_brace)
            temp_parsed_dict=json.loads(re.sub(r'\\', '/', outer_brace))
        except json.JSONDecodeError as e:
            print(e, f"{outer_brace} cannot be converted to dictionaty")
            continue
        if temp_parsed_dict:   # 一般来说是url或者pdf
            parsed_data_list.append(temp_parsed_dict)
    return parsed_data_list        

qwen_base_url="https://dashscope.aliyuncs.com/compatible-mode/v1" 
qwen_client=openai.OpenAI(base_url=qwen_base_url,api_key=secret_dict["qwen"],timeout=60)  

from openai import OpenAI
import os

def get_qwen_embedding(
    text: str,
    model: str = "text-embedding-v3",
    dimensions: int = 512
) -> list:
    """
    获取通义千问的文本嵌入向量
    
    参数:
    text -- 需要编码的文本内容（必填）
    model -- 模型名称（默认text-embedding-v3）
    dimensions -- 向量维度（默认1024，可选512/1024/1536）
    encoding_format -- 编码格式（默认float）
    
    返回:
    list -- 文本嵌入向量
    """
    # 长度限制
    if len(text)<1 or len(text)>8191:
        return []
    
    try:
        response = qwen_client.embeddings.create(
            model=model,
            input=text,
            dimensions=dimensions,
            encoding_format="float"
        )
        return response.data[0].embedding
    except Exception as e:
        print(f"Error generating embedding: {e}")
        return None
    
def ask_qwen_with_gpt_backup(prompt_text,history=[],system_instruction="",model="qwen-turbo",mode="str",temperature=0, enable_search=False, enable_citaton=False, retry_model="gpt-4o"):
    "优先使用Qwen，如果没有返回结果就问OpenAI"
    qwen_result=ask_qwen(prompt_text, history, system_instruction, model, mode, temperature, enable_search, enable_citaton)
    if qwen_result:
        return qwen_result
    else:
        gpt_result=ask_gpt(prompt_text=prompt_text, history=history, system_instruction=system_instruction, model=retry_model, mode="json", temperature=temperature)
        if gpt_result:
            print(f"Retried successful with {retry_model}")
            return gpt_result

def ask_qwen(prompt_text,history=[],system_instruction="",model="qwen-turbo",mode="str",temperature=0, enable_search=False, enable_citaton=False):
    "直接返回字符串"
    if mode=="json":
        response_format={ "type": "json_object" }
    else:
        response_format=None
    message=[{"role": "system", "content": system_instruction}]    # 角色包含system、user和assistant三种
    for d in history:  # 把通义千问的历史格式转化为GPT的历史格式
        message.append({"role":"user","content":d["user"]})
        message.append({"role":"assistant","content":d["bot"]})
    message.append({"role":"user","content":prompt_text})
    try:
        completion = qwen_client.chat.completions.create(
            model=model,
            temperature=temperature,
            response_format=response_format,
            messages=message,
            extra_body={
                "enable_search": enable_search,
                "search_options": {
                    "enable_source": True,
                    "enable_citation": enable_citaton,
                    "citation_format": "[<number>]",
                    "forced_search": False
                    }
                }
            )
        # print(completion)
        if completion.choices:
            return completion.choices[0].message.content
        else:
            print("Qwen no reply")
    except Exception as e:
        print(f"Qwen error {e}")

gpt_client =openai.OpenAI(base_url="https://api.wlai.vip/v1",api_key=general_key,timeout=60)
def ask_gpt(prompt_text,history=[],system_instruction="",model="gpt-3.5-turbo",mode="json",temperature=0):
    if mode=="json":
        response_format={ "type": "json_object" }
    else:
        response_format=None
    message=[{"role": "system", "content": system_instruction}]    # 角色包含system、user和assistant三种
    for d in history:  # 把通义千问的历史格式转化为GPT的历史格式
        message.append({"role":"user","content":d["user"]})
        message.append({"role":"assistant","content":d["bot"]})
    message.append({"role":"user","content":prompt_text})
    try:
        completion = gpt_client.chat.completions.create(
            model=model,
            # model='gpt-4-1106-preview',
            temperature=temperature,
            response_format=response_format,
            messages=message)
        # print(completion)
        if completion.choices:
            return completion.choices[0].message.content
        else:
            print("GPT no reply")
    except Exception as e:
        print(f"gpt error {e}")
        
import mimetypes

def mitaso_upload_file(cfid, file_path, api_key):
    """
    上传文件到指定的目录ID下。

    参数:
    - dir_id (str): 目录ID是文件要被上传到的在线路径，可以通过打开专题知识库的链接查看，如https://metaso.cn/subject/8580814774167015424/manage?cfid=8580815096909144064中，一个是专题，一个是目录cfid
    - file_path (str)：是文件的本地路径
    - api_key (str): 用户的API密钥。
    
    返回:
    - str: 服务器响应文本。
    """
    # 构建请求URL
    url = f"https://metaso.cn/api/open/file/{cfid}"
    
    # 设置请求头
    headers = {
        "Authorization": f"Bearer {api_key}",
    }
    
    # 获取文件名和类型
    file_name = file_path.split('/')[-1]
    file_type, _ = mimetypes.guess_type(file_name)
    
    # 打开并读取文件
    with open(file_path, "rb") as file:
        # 发送PUT请求上传文件
        response = requests.put(url, files={"file": (file_name, file, file_type)}, headers=headers)
        
    return response.text

if __name__=="__main__":  # 报告可用的gemini版本
    # for m in genai.list_models():
    #     if 'generateContent' in m.supported_generation_methods:
    #         print(m.name)
    response=ask_gpt("宇宙的尽头有什么",mode="str")
    print(response)
 