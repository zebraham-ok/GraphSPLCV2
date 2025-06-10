from numpy import argmin,argmax
from numpy import isnan
from langdetect import detect_langs
import re

def detext_lang_2(text:str):
    "检查并返回2位表示文本语言的字符串"
    try:
        result=detect_langs(text)[0].lang[:2]
        return result
    except Exception as e:
        return None
    
def transform_company_id(company_id):
    temp_str=company_id.split(" ")
    return "_".join([temp_str[2],temp_str[0],temp_str[1]])

# 中国新闻检索
def is_chinese_char(char):
    """判断单个字符是否是中文字符。"""
    if '\u4e00' <= char <= '\u9fff':
        return True
    return False

def is_chinese(text):
    """如果字符串中包含至少一个中文字符，则认为它是中文。"""
    for char in text:
        if is_chinese_char(char):
            return True
    return False

import re
def is_pure_english(text): # 注意，这里不能包含特殊符号
    pattern = r"[a-zA-Z\s]+"
    if re.fullmatch(pattern, text):
        return True
    else:
        return False

def get_shortest(name_list):
    index=argmin([len(i) for i in name_list])
    return name_list[index]

def get_longest(name_list):
    index=argmax([len(i) for i in name_list])
    return name_list[index]

def delete_null_dict_value(d:dict)->dict:
    "这个函数可以删除一个字典中的None,np.nan和空字符串"
    new_dict={}
    for key,value in d.items():
        if value is None:
            continue
        elif isinstance(value,float):
            if isnan(value):
                continue
        elif value=="":
            continue
        new_dict[key]=value
    return new_dict

def multi_split(s, separators):
    """
    根据多个分隔符分割字符串。
    
    参数:
        s (str): 要分割的字符串。
        separators (list of str): 包含分隔符的列表。
        
    返回:
        list: 分割后的字符串列表。
    """
    if not separators:
        return [s]  # 如果没有分隔符，则返回整个字符串作为一个元素的列表
    
    # 创建正则表达式模式，以匹配任何提供的分隔符
    pattern = '|'.join(map(re.escape, separators))
    
    # 使用 re.split() 函数根据正则表达式模式分割字符串
    result = re.split(pattern, s)
    
    # 移除空字符串（如果有的话），例如当两个分隔符相邻时可能会产生空字符串
    result = [item for item in result if item]
    
    return result

def get_text_snippet(long_str, small_str, min_span, max_span):
    "查找一个长字符串中，短字符串前后一定范围内的文本并返回，如果短字符串不存在则返回None"
    # 找到 small_str 在 long_str 中的起始位置
    start_idx = long_str.find(small_str)
    if start_idx == -1:
        return None  # 如果没有找到small_str，返回None
    
    # 获取前后截取的范围
    before_start = max(0, start_idx - max_span)
    after_end = start_idx + len(small_str) + max_span
    
    # 截取前min_span~max_span字符
    before_snippet = long_str[before_start:start_idx]
    after_snippet = long_str[start_idx + len(small_str):after_end]
    
    # 向前查找分隔符
    def find_before_break_point(snippet, min_span, max_span):
        # 查找从后往前的句号或者换行符
        match = re.search(r'[.|\n]', snippet[-max_span:])  # 从后往前查找
        if match:
            return snippet[-(max_span - match.start()):]
        else:
            return snippet[-max_span:]
    
    # 向后查找分隔符
    def find_after_break_point(snippet, min_span, max_span):
        # 查找从前往后的句号或者换行符
        match = re.search(r'[.|\n]', snippet[:max_span])  # 从前往后查找
        if match:
            return snippet[:match.end()]
        else:
            return snippet[:max_span]
    
    # 对前部分和后部分分别处理
    before_result = find_before_break_point(before_snippet, min_span, max_span)
    after_result = find_after_break_point(after_snippet, min_span, max_span)
    
    # 拼接结果
    return before_result + small_str + after_result

# News 检索的参数设定方法
params_template  = {
    # "q": search_term, 
    "textDecorations": True, 
    "textFormat": "HTML",
    "count":100,
    # "mkt":"zh-CN",  # 也可以翻译为英文，用en-US（但是这个不如直接改搜索关键词的语言效果好）
    "category":"Business",
    "sortBy":"Relevance",
    # "since":2015
    }

search_term_dict={  # 通过更改语言来搜索不同国家和地区的信息，未来应该引入日语、韩语、德语
    "简体中文":["合作伙伴","生产商","供货","供应商","采购"],
    "繁体中文":["合作夥伴","生產商","供貨","供應商","採購"],
    "英语":["partner","supplier","vendor","customer","purchase"],
    "日语":["パートナー","メーカー","サプライヤ","仕入先","仕入れ"],  # 日语要与英语和繁体中文分别匹配一次
    "韩语":["파트너", "생산업체", "납품", "공급업체", "구매"],
    "德语":["Lieferant","Kunde","Einkauf"],
}

sector_search_dict={
    "设计":["EDA","fab"],
    "制造":["炉","设备","光刻","湿电子","抛光","硅"],
    "元件":["硅"],
    "封装":["设备","仪器"],
}