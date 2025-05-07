"""
    本代码用来给Neo4j中的EntityObj添加country，description两个属性
    并再一次检查它是否有潜在的更完整名称，对潜在同义词进行“ALsoNamed”标注
    原来的EntityDes代码的信息完全依赖通义千问的内部enable_search，并无查证，因此生成的内容可能并不十分可靠
    本代码基于Google检索及其内部的知识图谱，给出更加可靠的回答
    是否要有Product和Can_Produce?
"""
import json
import time
from datetime import datetime
import API.ai_ask
import API.neo4j_SPLC
from API.Mongo_SPLC import MongoDBManager
from API.liang_google_search import LiangGoogleAPI4Company
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from text_process.find_json import get_dict_from_str

liang_host=LiangGoogleAPI4Company(default_collection="google_company")
mongo_host=MongoDBManager()

# 导入分类体系
with open("info\ic_category.json", encoding="utf8") as f:
    ic_category=json.load(f)
    
with open("info\ic_fab_category.json", encoding="utf8") as f:
    ic_fab_category=json.load(f)
    
ic_fab_category_industry_1=list(ic_fab_category.keys())

def get_google_info(company_name):
    "获取一个公司在Google检索首页的有效信息，并将谷歌检索结果进行整理"
    data=liang_host.execute_search_with_mongo(query=company_name, num=25,convert_bing=False)
    extracted = []
    
    # 处理organic部分的网页标题和描述
    if 'organic' in data:
        for item in data['organic']:
            title = item.get('title', '')
            description = item.get('description', '')
            if title or description:
                extracted.append(f"title: {title}\ndescription: {description}")
    
    # 处理top_stories中的问答对
    if 'people_also_ask' in data:
        for question_dict in  data['people_also_ask']:
            answer_source= question_dict.get("answer_source", "")
            question=question_dict.get("question", "")
            answer=",".join([i["value"].get("text") for i in question_dict.get("answers", []) if i["type"]=="answer"])
            if answer:
                extracted.append(f"question: {question}\nanswer_source: {answer_source}\nanswer: {answer}")
    return extracted

def entity_enrichment_module(neo4j_host: API.neo4j_SPLC.Neo4jClient, batch_size=50, max_workers=4):
    """实体信息增强模块"""
    # 获取需要处理的实体批次
    entity_batch = get_high_degree_entities(neo4j_host, batch_size)
    
    # 并行处理
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_single_entity, neo4j_host, entity) 
                 for entity in entity_batch]
        
        for future in tqdm(as_completed(futures), total=len(futures), desc="实体增强处理"):
            try:
                result = future.result()
            except Exception as e:
                print(f"处理失败: {str(e)}")

def get_high_degree_entities(neo4j_host, limit=1000):
    """获取高度中心性的实体"""
    query = '''
        MATCH (n)-[r:SupplyProductTo]-(m)
        WHERE n.name is not null AND size(labels(n)) > 0 and n.industry_1st is null
        RETURN n.name AS name, 
               labels(n) as labels, 
               elementId(n) as id, 
               COUNT(DISTINCT m) AS degree
        ORDER BY degree DESC
        LIMIT $limit
    '''
    result = neo4j_host.execute_query(query, parameters={"limit": limit})
    return [dict(record) for record in result]

def process_single_entity(neo4j_host, entity_data):
    """处理单个实体"""
    entity_id = entity_data["id"]
    entity_name = entity_data["name"]
    labels = [label for label in entity_data["labels"] if label != "EntityObj"]
    primary_label = labels[0] if labels else "Unknown"
    
    google_info_extracted=get_google_info(entity_name)
        
    # 获取AI增强信息
    ai_cate = get_ai_enriched_category(entity_name, enrich_info=google_info_extracted)
    ai_info = get_ai_enriched_info(entity_name, primary_label, enrich_info=google_info_extracted)
    # print(ai_info)
    # print(ai_cate)
    
    if (not ai_cate) or (not ai_info):
        # 对于无法生成描述的实体进行标记，免得每次都要重新处理一遍
        print("ai no response")
        neo4j_host.execute_query("match (n:EntityObj) where elementid(n)=$id set n.industry_1st=false", parameters={"id": entity_id})
        return
    
    industry_1=ai_cate.get("industry_1st", "")
    industry_2=ai_cate.get("industry_2nd", "")
    
    # 先将这个全名取出来，以免被更新到neo4j中
    full_cn_name=ai_info.pop("full_cn_name")
    
    # 更新节点属性
    all_data={**ai_info, **ai_cate}
    if industry_1 in ["无晶圆设计企业(Fabless)", "芯片制造", "电子元件", "制造业应用"] and industry_2 not in ["晶圆代工厂(Foundry)", "新能源", "航空航天/国防", "机械", "汽车制造", "机器人"]:
        chip_type_cate=ai_chip_type_check(entity_name, enrich_info=google_info_extracted)
        all_data.update(chip_type_cate)
    else:
        print(f"{entity_name} 是 {industry_1} 企业，不是芯片企业")
        
    # print(json.dumps(all_data,ensure_ascii=False, indent=2))
    update_node_properties(neo4j_host, entity_id, all_data)
    
    # 之前已经处理过一次了，暂时先注释掉这两行
    # if full_cn_name not in [entity_name, "", "未知", "不明"]:
    #     handle_same_entity_relation(neo4j_host, entity_id, full_cn_name)
    # print("———————————"*10)

from functools import wraps
def retry(max_retries=3, exceptions=(json.JSONDecodeError, KeyError)):
    """
    重试装饰器，支持自定义重试次数和捕获异常类型
    :param max_retries: 最大重试次数（默认3次）
    :param exceptions: 需要捕获的异常类型（默认JSONDecodeError和KeyError）
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    print(f"第{attempt}次尝试失败: {str(e)}")
                    if attempt == max_retries:
                        print(f"达到最大重试次数{max_retries}")
                        return None
            return None
        return wrapper
    return decorator

@retry(max_retries=3)
def get_ai_enriched_category(entity_name, enrich_info=""):
    prompt_text=f'''
        {enrich_info}
        上面是一些可能与【{entity_name}】有关的信息
        下面是一个二级分类体系，描述了集成电路产业相关的主要企业类型：
        {ic_fab_category}
        其中，垂直整合制造商(IDM)既做芯片设计又自有芯片制造工厂，Foundry专注于芯片代工，Fabless企业只做芯片设计不做制造。
        回答的具体json格式如下所示：
        {{
            "description": str,
            "is_included": bool, 
            "analysis_1": str,
            "analysis_2": str,
            "industry_1st": str,
            "industry_2nd": str,
            "other_possible_industry_2", list[str]
        }}
        1. 请根据开头的文本信息，用两三句中文描述【{entity_name}】的业务领域、所属行业、业务模式等基本信息，写在description位置；
        2. 请判断，该企业是否可以被上述分类体系所涵盖，请在is_included处用bool值进行判断；
        3. 请思考【{entity_name}】属于哪一个一级类别，并给出你的分析过程在analysis_1处（用“”引号引用原文来分析）；
        4. 请思考该企业属于哪些二级类别，并给出你的分析过程在analysis_2处（用“”引号引用原文来分析）；
        5. 请从上述分类体系中，选出【{entity_name}】最符合的一级类别（仅1个），写在industry_1st处；
        6. 请从上述分类体系中，选出该企业最符合的二级类别（仅1个），写在industry_2nd处；
        7. 如果你认为上面这一个industry_2nd不足以描述该企业的完整特点，请找出其它你认为也适合描述该企业的行业属性的二级类别，以list of string的形式写在other_possible_industry_2位置。如果你认为不需要，请返回空列表。
        注意：你提供的信息必须依据原文材料，真实、准确、可靠。
    '''
    # 实际上设备企业经常可以生产多种设备
    response = API.ai_ask.ask_qwen_with_gpt_backup(
        prompt_text=prompt_text,
        system_instruction="你是一个商业信息情报员，提供json格式的准确结构化数据",
        temperature=0,
        enable_search=True,
        mode="json",
        model="qwen-plus",
        retry_model="gpt-4o"
    )
            
    if not response:
        print(f"description for {entity_name} generation failed")
        return None
    
    try:
        ai_dict=get_dict_from_str(response)[0]
    except Exception as e:
        print(f"Error in get_ai_enriched_category {e}")
        return None
        
    validated_dict=validate_ai_cate_response(ai_dict)
    return validated_dict

def validate_ai_cate_response(data: dict):
    """验证AI返回数据结构"""
    
    required_fields = ["description", "is_included", "analysis_1", "analysis_2", "industry_1st", "industry_2nd"]
    
    if not all(field in data for field in required_fields):
        raise ValueError("缺少必要字段")
    
    analysis_1=data.pop("analysis_1")
    analysis_2=data.pop("analysis_2")
    # analysis=str(analysis_1)+"\n"+str(analysis_2)
    # data["analysis"]=analysis
    contain_stock_info=data.pop("is_included")
    
    # description=data["description"]
    # if isinstance(description, str) and len(description)>300:
    #     data["description"]=description[:300]
    
    if contain_stock_info:
        industry_1st=data.get("industry_1st")
        industry_2nd=data.get("industry_2nd")
        if not industry_1st:   # 应该增加一个验证，确认这些类别是否是我们规定的类别
            raise ValueError("缺少industry_1st")
        if industry_1st not in ic_fab_category_industry_1:
            print(f"{industry_1st} 不在预设的分类体系中")
        if not industry_2nd:
            raise ValueError("缺少industry_2nd")
    return data

# 从Category信息中，选择需要进行芯片类型细分的，进行自动化细分
@retry(max_retries=3)
def ai_chip_type_check(entity_name, enrich_info):
    prompt_text=f'''
        {enrich_info}
        上面是一些可能与【{entity_name}】有关的信息
        下面是一个三级分类体系，描述了主要的集成电路产品类型：
        {ic_category}
        回答的具体json格式如下所示：
        {{
            "analysis": str,
            "can_find_category": bool, 
            "category_1st": str,
            "category_2nd": str,
            "category_3rd": str,
            "other_possible_category_3rd", list[str]
        }}
        1. 请根据开头的文本信息，分析【{entity_name}】的可以生产哪些类型的集成电路，写在analysis位置；
        2. 请首先判断，该企业是否生产上述三级分类中的产品？请在can_find_category处用bool值进行判断；
        3. 请思考【{entity_name}】的首要产品属于哪一个一级类别？写在category_1st处（仅1个）；
        4. 请思考【{entity_name}】的首要产品属于哪一个二级类别？写在category_2nd处（仅1个）；
        5. 请思考【{entity_name}】的首要产品属于哪一个三级类别？写在category_3rd处（仅1个）；
        6. 如果你认为该企业在此之外还有其它集成电路产品，请找出其它产品的的三级类别，以list of string的形式写在other_possible_category_3rd位置。如果你认为不需要，请返回空列表。
        注意：你提供的信息必须依据原文材料，真实、准确、可靠。
    '''
    # 实际上设备企业经常可以生产多种设备
    response = API.ai_ask.ask_qwen_with_gpt_backup(
        prompt_text=prompt_text,
        system_instruction="你是一个商业信息情报员，提供json格式的准确结构化数据",
        temperature=0,
        enable_search=True,
        mode="json",
        model="qwen-plus",
        retry_model="gpt-4o"
    )
            
    if not response:
        print(f"description for {entity_name} generation failed")
        return None
    
    try:
        ai_dict=get_dict_from_str(response)[0]
    except Exception as e:
        print(f"Error in ai_chip_type_check {e}")
        return None
        
    validated_dict=validate_ai_chip_response(ai_dict)
    return validated_dict

def validate_ai_chip_response(data: dict):
    required_fields = ["analysis", "can_find_category", "category_1st", "category_2nd"]
    
    if not all(field in data for field in required_fields):
        raise ValueError("缺少必要字段")
    
    can_find_category=data.pop("can_find_category")
    analysis=data.pop("analysis")  # 删掉analysis不让它进入后续的处理阶段
    # print(analysis)
    
    if can_find_category:
        return data
    
# 使用装饰器的函数示例
@retry(max_retries=3)
def get_ai_enriched_info(entity_name, label, enrich_info=""):
    """获取AI增强信息（带重试机制）"""
    prompt_text=f'''
        {enrich_info}
        请你根据上述信息，告诉我【{entity_name}】是一个什么样的{label}
        回答的具体json格式如下所示：
        {{
            "analysis": str, 
            "full_cn_name": str,
            "synonym_name_list": list[str]
            "country": str,
            "contain_stock_info": bool,
            "stock_ticker_list", list[str],
            "stock_code_list": list[str],
        }}
        1. 请将你对后续问题的分析过程写在analysis位置
        2. 如果你认为{entity_name}不是该实体的正式全称，请在full_cn_name处给出其正式全称，并尽量使用中文。如果实在没有合适的中文名，也可以使用原来语言的正式名称；
        3. 在country位置，请用中文给出企业所在国家或地区的中文简称，如“美国”、“日本”、“中国台湾”，“中国大陆”等；
        4. 在synonym_name_list处，请你用中文、英文以及该企业所在国家的语言，给出它的其它常见名称同义词；
        5. 该企业是否是上市公司？如果原文中包含{entity_name}的股票代码信息，请在contain_stock_info处返回true，否则返回false；
        6. 一个公司可能在不同地方上市从而有多个股票标签，如果原文确实包含{entity_name}的股票ticker信息，请将它的Token写在stock_ticker_list处（如：["NVDA"]和["贵州茅台"]），否则返回空列表；
        7. 如果原文确实包含{entity_name}的股票代码，请将它的股票代码以list of string的形式写在stock_code_list处（以数据.市场代码的格式，如：0000.KS或XXXX.NAS或1111.SH），否则请在stock_code_list处返回空列表。
        注意：你提供的信息必须依据原文材料，真实、准确、可靠。
    '''
            
    response = API.ai_ask.ask_qwen_with_gpt_backup(
        prompt_text=prompt_text,
        system_instruction="你是一个商业信息情报员，提供json格式的准确结构化数据",
        temperature=0,
        enable_search=True,
        mode="json",
        model="qwen-turbo",
        retry_model="gpt-3.5-turbo"
    )
            
    if not response:
        print(f"description for {entity_name} generation failed")
        return None
    
    try:
        ai_dict=get_dict_from_str(response)[0]
        return ai_dict
    except Exception as e:
        print(f"Error in get_ai_enriched_info {e}")
        return {}
        

def validate_ai_info_response(data: dict):
    """验证AI返回数据结构"""
    
    required_fields = ["analysis", "full_cn_name", "country", "contain_stock_info"]
    
    if not all(field in data for field in required_fields):
        raise ValueError("缺少必要字段")
    
    data.pop("analysis")
    contain_stock_info=data.pop("contain_stock_info")
    
    # description=data["description"]
    # if isinstance(description, str) and len(description)>300:
    #     data["description"]=description[:300]
    
    if contain_stock_info:
        if "stock_ticker_list" in data and (not data.get("stock_ticker_list", [])):
            del data["stock_ticker_list"]
        if "stock_code_list" in data and (not data.get("stock_code_list", [])):
            del data["stock_code_list"]
    return data

def update_node_properties(neo4j_host: API.neo4j_SPLC.Neo4jClient, entity_id: str, ai_info: dict):
    """更新节点属性"""
    # query = '''
    #     MATCH (n) 
    #     WHERE elementId(n) = $id
    #     SET n.country = $country,
    #         n.description = $description,
    #         n.updated_at = datetime()
    # '''  # 可以通过把一个属性写在这里来控制是否生成这个属性
    # neo4j_host.execute_query(query, parameters={
    #     "id": entity_id,
    #     **ai_info        
    # })
    allowed_keys=["stock_code_list", "stock_ticker_list", "full_cn_name", "country", 
                  "category_1st", "category_2nd", 
                  "description", "industry_1st", "industry_2nd"]
    ai_info["updated_at"]=datetime.now()
    update_result=neo4j_host.Update_node(update_attr_dict=ai_info, identifier_value=entity_id)
    # print(update_result)

def handle_same_entity_relation(neo4j_host: API.neo4j_SPLC.Neo4jClient, source_id, full_cn_name):
    """处理疑似相同实体关系"""
    # 查找目标节点
    target_node = neo4j_host.execute_query('''
        MATCH (n:EntityObj {name: $name})
        RETURN elementId(n) as id LIMIT 1
    ''', parameters={"name": full_cn_name})
    
    if target_node:
        neo4j_host.Crt_rel_by_id(source_id, target_node[0]["id"], relationship_type="FullNameIs")

def entity_des_main(neo4j_host=None, max_worker=4):
    "为实体生成描述"
    if not neo4j_host:
        neo4j_host=API.neo4j_SPLC.Neo4jClient(driver=API.neo4j_SPLC.local_driver)
    
    while True:
        entity_enrichment_module(neo4j_host, batch_size=1000, max_workers=max_worker)
        print("当前全部描述已生成，休眠十分钟")
        time.sleep(600)
        
# 使用示例
if __name__ == "__main__":
    from main.EntityDesGoogle import entity_des_main
    from Neo4jHost import get_remote_driver

    neo4j_host=get_remote_driver()
    entity_des_main(neo4j_host)