"主要包含了generate_search_term和get_now_time两个支持性的函数"
import json
from text_process.languange import is_chinese
import re
from numpy import isnan

def is_not_nan(value):
    "判断一个值是不是nan或者空内容"
    if isinstance(value,float):
        if isnan(value):
            return False
        else:
            return True
    else:
        if value:
            return True
        else:
            return False 
        
def remove_brackets_content(text):
    "匹配各种括号内的内容，包括 ()、[]、{}、<> 以及中文括号 （）、【】、《》"
    pattern = r'\([^)]*\)|\{[^}]*\}|\[[^\]]*\]|<[^>]*>|（[^）]*）|【[^】]*】|《[^》]*》'
    # 使用re.sub()替换匹配的内容为空字符串
    result = re.sub(pattern, '', text)
    return result

def is_semicon_comp_dict(node_dict):
    "给定一个节点的property dict，返回这个节点是否是微电子相关"
    json_txt=json.dumps(node_dict,ensure_ascii=False)
    for i in ["芯","电","半导体","矽","emico","chip","IC","ircuit"]:
        if i in json_txt:
            return True
    return False

# 这个函数是希望未来能够替代上面的函数，从而更方便地将研究的行业切换为其它的
def is_this_industry_comp_dict(node_dict,industry_symbol_list=["芯","电","半导体","矽","emico","chip","IC","ircuit"]):
    "给定一个节点的property dict，返回这个节点是否是微电子相关"
    json_txt=json.dumps(node_dict,ensure_ascii=False)
    for i in industry_symbol_list:
        if i in json_txt:
            return True
    return False

def generate_search_term(node_dict,industry_symbol_list,insert_term=None,insert_zh_term=None,insert_en_term=None,insert_jp_term=None,insert_ks_term=None):
    """
        基于 node_dict 生成查询语句，考虑了语言差异，如果节点的 node_dict 信息已经能够体现它是我们所关注的行业了，则在检索词末尾添加对应语言的“供应商”作为引导词，如果不能够体现，则填在行业名称作为引导词
        insert_term 一旦被使用，就不管原节点是否具有行业属性，都会简单进行添加 insert_term 处理
    """
    go=True
    
    if insert_term and ('name' in node_dict):
        node_name=node_dict["name"]
        search_term=node_name+" "+insert_term
        return go,node_name,search_term
    
    if "createDate" in node_dict:
        del node_dict["createDate"]
    if "name_embedding" in node_dict:
        del node_dict["name_embedding"]
    
    if 'name' in node_dict and "country" in node_dict:
        node_name=node_dict["name"]
        node_country=node_dict["country"]
        if "日本" in node_country or "apan" in node_country:
            if is_this_industry_comp_dict(node_dict,industry_symbol_list):
                search_term=node_name+" "+insert_jp_term
            else:
                search_term=node_name+" "+"仕入れ"
        elif "韩" in node_country or "orea" in node_country:
            if is_this_industry_comp_dict(node_dict,industry_symbol_list):
                search_term=node_name+" "+insert_ks_term
            else:
                search_term=node_name+" "+"공급업체"
        else:
            if is_chinese(node_name):
                if is_this_industry_comp_dict(node_dict,industry_symbol_list):
                    search_term=node_name+" "+"供应商"
                else:
                    search_term=node_name+" "+insert_zh_term   # 这是一个非常初步的方法，未来应该考虑充分利用node_dict中已有的信息
            else:
                if is_this_industry_comp_dict(node_dict,industry_symbol_list):
                    search_term=node_name+" "+"Supplier"
                else:
                    search_term=node_name+" "+insert_en_term
    elif 'name' in node_dict:
        node_name=node_dict["name"]
        if is_chinese(node_name):
            if is_this_industry_comp_dict(node_dict,industry_symbol_list):
                search_term=node_name+" "+"供应商"
            else:
                search_term=node_name+" "+insert_zh_term
        else:
            if is_this_industry_comp_dict(node_dict,industry_symbol_list):
                search_term=node_name+" "+"Supplier"
            else:
                search_term=node_name+" "+insert_en_term
    else:
        go=False
        node_name=""
        search_term=""
        
    return go,node_name,search_term

from datetime import datetime
def get_now_time():
    "Get the current date and time"
    now = datetime.now()
    formatted_date_time = now.strftime("%Y-%m-%d %H:%M")
    return formatted_date_time

def print_columns(*lists):
    if not lists:
        return
    
    list_length = len(lists[0])
    if any(len(lst) != list_length for lst in lists):
        raise ValueError("All input lists must have the same length.")
    
    # Calculate the maximum width for each column
    max_widths = []
    for col in range(list_length):
        max_width = max(len(str(item)) for lst in lists for item in lst[col:col+1])
        max_widths.append(max_width)
    
    # Print the formatted output
    for i in range(list_length):
        line = ""
        for j, lst in enumerate(lists):
            # Left justify and add spacing between columns
            line += "{:<{width}} ".format(lst[i], width=max_widths[j])
        print(line.strip())  # Remove trailing space

# Example usage
if __name__=="__main__":
    names = ["Alice", "Bob", "Charlie", "David"]
    ages = [24, 28, 32, 21]
    salaries = [50000, 60000, 70000, 45000]

    print_columns(names, ages, salaries)