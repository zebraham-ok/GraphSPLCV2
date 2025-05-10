"""
    本代码用来给Neo4j中的EntityObj添加country，description两个属性，并再一次检查它是否有潜在的更完整名称
    经过这个代码处理之后，基本上可以消除同义词问题（因为在建立EntityObj的时候，已经强调一次简称问题了，这里是双保险）
    但是，目前本代码的信息完全依赖通义千问的内部enable_search，并无查证，因此生成的内容可能并不十分可靠
"""
import json
import time
import API.ai_ask
import API.neo4j_SPLC
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

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
        WHERE n.name is not null AND size(labels(n)) > 0 and n.description is null
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
    
    # 获取AI增强信息
    ai_info = get_ai_enriched_info(entity_name, primary_label)
    if not ai_info:
        # 对于无法生成描述的实体进行标记，免得每次都要重新处理一遍
        neo4j_host.execute_query("match (n:EntityObj) where elementid(n)=$id set n.description=false", parameters={"id": entity_id})
        return
    
    # 更新节点属性
    update_node_properties(neo4j_host, entity_id, ai_info)
    
    # 处理疑似相同实体
    if ai_info["full_cn_name"] not in [entity_name, "", "未知", "不明"]:
        handle_same_entity_relation(neo4j_host, entity_id, ai_info["full_cn_name"])

def get_ai_enriched_info(entity_name, label):
    """获取AI增强信息（带重试机制）"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            prompt_text=f'''
                请你根据检索到的信息，告诉我【{entity_name}】是一个什么样的{label}
                回答的具体json格式如下所示：
                {{
                    "name": {entity_name}, 
                    "full_cn_name": str,
                    "country": str,
                    "description": str
                }}
                其中，name处请重复我给你的实体名称。如果你认为它不是该实体的正式中文全称，请在full_cn_name处给出其正式全称尽量使用中文。如果实在没有合适的中文名，也可以使用原来语言的正式名称；
                在country位置，请用中文给出企业所在国家或地区的中文简称，如“美国”、“日本”、“中国台湾”，“中国大陆”等；
                在description位置，请用一两句话，介绍该企业，以所属行业、主营业务、商业模式为重点；
                注意：你提供的信息必须要真实、准确、可靠。
            '''
            
            response = API.ai_ask.ask_qwen_with_gpt_backup(
                prompt_text=prompt_text,
                system_instruction="你是一个商业信息情报员，提供json格式的准确结构化数据",
                temperature=0.05,
                enable_search=True,
                mode="json"
            )
            
            if not response:
                print(f"description for {entity_name} generation failed")
                return None
                
            return validate_ai_response(json.loads(response), entity_name)
            
        except (json.JSONDecodeError, KeyError) as e:
            print(f"第{attempt+1}次解析失败: {str(e)}")
            continue
            
    print(f"无法获取 {entity_name} 的有效信息")
    return None

def validate_ai_response(data, original_name):
    """验证AI返回数据结构"""
    required_fields = ["name", "full_cn_name", "country", "description"]
    
    if not all(field in data for field in required_fields):
        raise ValueError("缺少必要字段")
        
    # 让大模型重复一遍name只是怕它出错，并不需要验证
    # if data["name"] != original_name:
    #     raise ValueError(f"{data['name']}与原来的实体名称{original_name}不匹配")
        
    return {
        "full_cn_name": data["full_cn_name"],
        "country": data["country"],
        "description": data["description"][:200]  # 限制长度
    }

def update_node_properties(neo4j_host, entity_id, ai_info):
    """更新节点属性"""
    query = '''
        MATCH (n) 
        WHERE elementId(n) = $id
        SET n.country = $country,
            n.description = $description,
            n.updated_at = datetime()
    '''
    neo4j_host.execute_query(query, parameters={
        "id": entity_id,
        # "full_cn_name": ai_info["full_cn_name"],
        "country": ai_info["country"],
        "description": ai_info["description"]
    })

def handle_same_entity_relation(neo4j_host: API.neo4j_SPLC.Neo4jClient, source_id, full_cn_name):
    """处理疑似相同实体关系"""
    # 查找目标节点
    target_node = neo4j_host.execute_query('''
        MATCH (n:EntityObj {name: $name})
        RETURN elementId(n) as id LIMIT 1
    ''', parameters={"name": full_cn_name})
    
    if target_node:
        neo4j_host.Crt_rel_by_id(source_id, target_node[0]["id"], relationship_type="FullNameIs", set_date=False)

def entity_des_main(neo4j_host=None, max_worker=2):
    "为实体生成描述"
    if not neo4j_host:
        neo4j_host=API.neo4j_SPLC.Neo4jClient(driver=API.neo4j_SPLC.local_driver)
    
    while True:
        entity_enrichment_module(neo4j_host, batch_size=1000, max_workers=max_worker)
        print("当前全部描述已生成，休眠十分钟")
        time.sleep(600)
        
# 使用示例
if __name__ == "__main__":
    entity_des_main()