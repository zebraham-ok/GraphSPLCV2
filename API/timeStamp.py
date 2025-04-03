import re
from datetime import datetime
# from datetime import date

# 当前时间
current_time = datetime.now()
forbid_time=datetime(year=2050,month=1,day=1)

def format_year_month(text):
    "匹配 `年份-月份` 或 `月份-年份` 格式"
    pattern = r'(\d{4}年\d{1,2}月)|(\d{4})[-/\.](\d{1,2})|(\d{1,2})[-/](\d{4})'
    match = re.search(pattern, text)
    if match:
        if match.group(1):  # 处理类似 "2024年10月"
            return match.group(1).replace('年', '-').replace('月', '')
        elif match.group(2) and match.group(3):  # 处理类似 "2024-10" 或 "2024/10"
            return f"{match.group(2)}-{match.group(3).zfill(2)}"
        elif match.group(4) and match.group(5):  # 处理类似 "10-2024" 或 "10/2024"
            return f"{match.group(5)}-{match.group(4).zfill(2)}"  # 调整为 "2024-10"

# 大写数字映射
chinese_num_map = {'一': '1','二': '2','三': '3','四': '4'}
def convert_chinese_num(num_str):
    "转换大写数字为阿拉伯数字"
    return chinese_num_map.get(num_str, num_str)

def standardize_quarter(quarter_str):
    "定义一个正则表达式模式来匹配多种格式的季度字符串"
    patterns = [
        (r'(\d{4})年(第)?(\d)季度', r'\1-Q\3'),  # 匹配 "2019年第3季度" 格式
        (r'(\d{2})年(第)?(\d)季度', r'20\1-Q\3'),  # 匹配 "19年3季度" 格式
        (r'(\d{4})年(第)?(一|二|三|四)季度', lambda m: f'{m.group(1)}-Q{convert_chinese_num(m.group(3))}'),  # 匹配 "2019年三季度" 格式
        (r'(\d{2})年(第)?(一|二|三|四)季度', lambda m: f'20{m.group(1)}-Q{convert_chinese_num(m.group(3))}'),  # 匹配 "19年三季度" 格式
        (r'(\d{4})年(第)?(\d)季', r'\1-Q\3'),      # 匹配 "2019年3季" 格式
        (r'(\d{2})年(第)?(\d)季', r'20\1-Q\3'),    # 匹配 "19年3季" 格式
        (r'(\d{4})Q(\d)', r'\1-Q\2'),          # 匹配 "2019Q3" 格式
        (r'(\d{2})Q(\d)', r'20\1-Q\2'),        # 匹配 "19Q3" 格式
        (r'(\d)Q(\d{2})', r'20\2-Q\1'),        # 匹配 "3Q19" 格式
        (r'(\d{4})-(\d)', r'\1-Q\2'),          # 匹配 "2019-3" 格式
        (r'(\d{4})年(\d)Q', r'\1-Q\2'),        # 匹配 "2019年3Q" 格式
        (r'(\d{2})年(\d)Q', r'20\1-Q\2'),      # 匹配 "19年3Q" 格式
    ]
    
    for pattern, replacement in patterns:
        match = re.search(pattern, quarter_str)
        if match:
            # 如果匹配成功，则使用指定的格式进行替换
            return re.sub(pattern, replacement, quarter_str)
    
    # 如果没有任何模式匹配，则返回原字符串
    return None

def parse_date(text,skip_range=False):
    text=str(text)
    "必须至少要有年份，返回的是一个字典，其中包含了since，until和time三个变量，既可能存储时间区间也可以存储点时间。如果skip_range=True则会自动忽略时间区间，仅能返回点时间，格式仍然为字典"
    text=text.replace("XX-","").replace("-XX","").replace("xx-","").replace("-xx","").replace("-Null","").replace("-NULL","").replace("-null","").replace("Null-","").replace("NULL-","").replace("null-","").replace("-N/A","").replace("N/A-","")
    text=text.strip("-").strip("/")
    
    result = {}

    # 1.1 处理英文日期区间（如 2021-09-29~2021-12-03）
    if not skip_range:
        range_pattern_list = [
            r'(\d{4}[-/]\d{1,2}[-/]\d{1,2})~(\d{4}[-/]\d{1,2}[-/]\d{1,2})',
            r'(\d{4}年\d{1,2}月\d{1,2}日)[~-](\d{4}年\d{1,2}月\d{1,2}日)',
            r'(\d{4}[-/]\d{1,2})~(\d{4}[-/]\d{1,2})',
            r'(\d{4}年\d{1,2}月)~(\d{4}年\d{1,2}月)',
            r'(\d{4})(?=年)[~-](\d{4})(?=年)',
            r'(\d{4})[~-](\d{4})',
        ]
        for range_pattern in range_pattern_list:
            range_match = re.search(range_pattern, text)
            if range_match:
                since_time_str = range_match.group(1)
                until_time_str = range_match.group(2)
                # print(text,range_match.groups())
                if since_time_str and until_time_str:
                    # print(range_match.group(1),range_match.group(2),range_match.group(0))
                    since_time=parse_date(since_time_str,skip_range=True)
                    until_time=parse_date(until_time_str,skip_range=True)
                    if ("date" in since_time) and ("date" in until_time):
                        result['since'] = since_time["date"]
                        result['until'] = until_time["date"]
                        return result
    
    # 2.1 处理年月日
    # 2.1.0处理ISO格式
    if "T" in text:
        try:  # 这里去掉了时区的信息，毕竟差一天问题不大
            date_obj=datetime.fromisoformat(text.split(" ")[0].replace("Z", ""))
            return {"date":date_obj.strftime("%Y-%m-%d")}
        except Exception as e:
            # print(f"{text} contains T but seems not an ISO format")
            pass
    elif len(text)==8:  # 处理六位数字格式
        try:
            int(text)
            date_obj = datetime.strptime(text, "%Y%m%d")
            result['date'] = date_obj.strftime("%Y-%m-%d")
            return result
        except Exception as e:
            pass
    # 2.1.1 处理非ISO格式
    full_date_pattern = r'(\d{4}年\d{1,2}月\d{1,2}日|\d{4}[-/\.]\d{1,2}[-/\.]\d{1,2}|\d{1,2}[-/\.]\d{1,2}[-/\.]\d{4})'
    full_date_match = re.search(full_date_pattern, text)
    if full_date_match:
        date_str = full_date_match.group(0)
        # 处理类似2024年
        try:
            if "年" in date_str:
                date_obj = datetime.strptime(date_str, "%Y年%m月%d日")
            else:
                date_str=date_str.replace("/","-")
                date_obj = datetime.strptime(date_str, "%Y-%m-%d")
                
            if date_obj<forbid_time:
                result['date'] = date_obj.strftime("%Y-%m-%d")
                return result
            else:
                return {}
        except Exception as e:
            print(f"Error with {date_str}")
            
    # 2.1.2 处理英文月份简写
    # 正则表达式模式，用于匹配日期字符串（兼容有.的情况）
    den_month_pattern = r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[.]{0,1}\s+\d{1,2},\s+\d{4}\b'
    en_month_match = re.search(den_month_pattern, text)
    if en_month_match:
        date_str = en_month_match.group(0)
        if "." in date_str:
            date_obj=datetime.strptime(date_str,"%b. %d, %Y")
        else:
            date_obj=datetime.strptime(date_str,"%b %d, %Y")
            
        if date_obj<forbid_time:
            result['date'] = date_obj.strftime("%Y-%m-%d")
            return result
        else:
            return {}
    
    # 2.1 处理季度
    if ("Q" in text) or ("季" in text):
        qurter_text=standardize_quarter(text)
        if qurter_text:
            return {"date":qurter_text}
    
    # 2.3 处理年月
    # 匹配格式：10月14日，2024/10，2024-10
    year_month_str=format_year_month(text)
    if year_month_str:
        try:
            year_month=datetime.strptime(year_month_str,"%Y-%m")
            # print(year_month)
            if year_month<forbid_time:
                return {"date":year_month.strftime("%Y-%m")}
            else:
                return {}
        except Exception as e:
            print(f"Error with {year_month_str}")
            
    # 2.4 处理年份（如 2024年）
    year_pattern = r'(\d{4})(?=年)|(\d{4})'
    year_match = re.search(year_pattern, text)
    if year_match:
        year=datetime.strptime(year_match.group(0),"%Y")
        if year<forbid_time:
            return {"date":year_match.group(0)}
        else:
            return {}
    
    # 3. 处理不明确日期（如 'unknown', 'N/A' 或 'XX-2023'）
    unknown_pattern = r'(Unknown|unknown|N/A|NA|NONE|None|none|null|Null|NULL)'
    unknown_match = re.search(unknown_pattern, text)
    if unknown_match:
        return result
    
    # 6. 处理没有时间信息（如 '无法从提供的信息中确定'）
    no_time_pattern = r'(\w+不|无|未)'
    no_time_match = re.search(no_time_pattern, text)
    if no_time_match:
        return result

    return result
       
# 以下是用于时间后续处理的代码

def get_year(date:str):
    "将一个YYYY-MM-DD格式的字符串日期转化为整数年份"
    year=date.split("-")[0]
    if len(year)==4:
        try:
            year=int(year)
            if 1000<year<3000:
                return year
        except Exception as e:
            print(year,e)

def get_year_from_list(str_list):
    "从一个list[str]中获取年份整数列表"
    years=[]
    for date in str_list:
        if isinstance(date,str):
            year=get_year(date)
            if year:
                years.append(year)
    return years

def extract_year(d:dict):
    "提取一个连边信息字典中的年份，并以列表的形式返回"
    if "since" in d and "until" in d:
        try:
            since=d["since"]
            until=d["until"]
            if isinstance(since,str) and isinstance(until,str):
                since=get_year(since)
                until=get_year(until)
                if since and until:
                    if since<=until:
                        return list(range(since,until+1))
        except Exception as e:
            print(f"failed to convert {since} and {until} because of {e}")
    if "time" in d:
        time=d["time"]
        if isinstance(time,str):
            time=get_year(time)
            if time:
                return [time]
    if "date" in d:
        date=d["date"]
        if isinstance(date,str):
            date=get_year(date)
            if date:
                return [date]
    
    return [None]

if __name__=="__main__":
    # 测试多个不规则时间字符串
    time_strings = ["2024-10-08T09:44:10.555 America/New_York","2024-10-08T09:44:10.555Z","Oct 31, 2024",
                    "2000年1月12日-2001年5月2日","10月14日", "2023/12/31", "08-2021", "2024-XX-XX", "1998年", "unknown", 
                    "2023/10/13", "2023/9/18", "2012-01-01~2015-12-31", "2021-09-29~2021-12-03", 
                    "2024/10-31", "11-2021", "Dec-70", "N/A", "2023年7月19日", "XX-2023", 
                    "2020年", "2023/10/6", "2021/01~2021/07", "2019-2025","暂不确定","1987",'2024-10-15T06:00:00Z',
                    "2024-10-30 18:15:46","2022年8月9日","2020年11月11日 11:10","20230908","Dec. 7, 2021","2023年1月","2024-2029年","2024-05-13T02:57:00.0000000Z","2019年第三季度", "20年一季度", "2019Q3", "19-3", "2019年3季", "19年3季度","3Q19"," 2022.12.30"]

    # 逐个处理时间字符串
    for ts in time_strings:
        print(f"Input: {ts} -> Output: {parse_date(ts)}")
