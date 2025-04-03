"用来获取Ai回答中的json"
import re
import json

def _find_outer_braces(s): 
    "获取所有的最外层大括号（避免因为没生成完导致缺少反括号]的错误）"
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
    return result

def _solve_nested_quotes(text):
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

def _process_json_response(outer_braces_list):
    "专门用来处理[str(dict), str(dict)...]形式的数据，将其变为真正的字典"
    parsed_data_list=[]
    for outer_brace in outer_braces_list:
        temp_parsed_dict={}
        
        try:   # 在尝试将字符串转化为字典的时候，要注意文本中可能存在的反斜杠(replace解决子，re可以)
            outer_brace=_solve_nested_quotes(outer_brace)
            outer_brace=outer_brace.replace("\n","").replace("  ","")  # 避免多余部分
            outer_brace=re.sub('''",}''','''"}''',outer_brace)
            outer_brace=re.sub('''", }''','''"}''',outer_brace)
            temp_parsed_dict=json.loads(re.sub(r'\\', '/', outer_brace))
            parsed_data_list.append(temp_parsed_dict)
            continue
        except json.JSONDecodeError as e:
            print(e, f"{outer_brace} cannot be converted to dictionaty")
            # continue
        try:
            temp_parsed_dict=eval(outer_brace.replace("null", "None").replace(": true", ": True"))
            if temp_parsed_dict and isinstance(temp_parsed_dict,dict):
                parsed_data_list.append(temp_parsed_dict)
                continue
        except Exception as e:  # eval是用Python运行程序，因此无法处理json中的null，而json.loads可以
            print(f"eval function failed with {e} trying solve nested quotes")
        
        # if temp_parsed_dict:   # 一般来说是url或者pdf
        #     parsed_data_list.append(temp_parsed_dict)
    return parsed_data_list

def get_dict_from_str(s):
    "真正有用的函数，返回[dict, dict...]"
    outer_braces_list=_find_outer_braces(s)
    return _process_json_response(outer_braces_list)