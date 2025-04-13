"使用大语言模型抽取企业生产和采购产品的信息"
from tqdm import tqdm
import json
import API.ai_ask
import API.neo4j_SPLC
import time

neo4j_host=API.neo4j_SPLC.Neo4jClient(driver=API.neo4j_SPLC.local_driver)
import concurrent.futures

def ai_product_recognition(article_title, section_content, entity_obj_name):
    "用大模型直接确认原文中的实体，分析这些实体是否是具体的，并给出它们的正式中文名称"
    # 这一步需要的中国特有知识较多，因此使用qwen更加准确
    ai_response=API.ai_ask.ask_qwen(prompt_text='''
            请查看如下文本片段，严格依照原文文本内容，按照如下步骤，结合你检索到的知识完成任务：
            {"title": %s, "section_content": %s, "entity_obj_name": %s}
            1. 请在本段落中找出所有被生产或采购的具体产品或服务（不包括会计性、融资性、政策性的内容），在all_products一栏以列表的形式返回一个list[str]，作为结果的第一项
            2. 上述产品在文中出现的背景是什么？是被生产还是被采购？请用自然语言对all_products中的每个产品在文中的背景进行分析，以一个str的形式放在analyzation一栏中，作为结果的第二项。
            3. 如果该段落中有代词代指具体的实体（如“本公司”、“该企业”、“该产品”等），请在pronoun_entity一栏列出，用一个字典形式说明其在原文中指代对象的正式中文全称，作为结果的第三项。
            4. 查看entity_obj_name中的组织，文中是否提到了它们可以自己生产这些产品门类或产品型号？如果有，请在entity_produce_product一栏，以dict[entity: product]的形式汇总，作为结果的第四项。
            5. 查看entity_obj_name中的组织，文中是否提到了它们需要采购的产品门类或产品型号？如果有，请在entity_need_product一栏，以dict[entity: list[product]]的形式汇总，作为结果的第五项。
            请以json格式汇总上述四项内容，格式如下所示：
            {
                "all_products": list[str],
                "pronoun_entity": dict[代词: 指代对象],
                "analyzation": str,
                "entity_produce_product": dict[entity_obj_name中的组织名称: list[all_products中的产品门类或产品型号]],
                "entity_need_product": dict[entity_obj_name中的组织名称: list[all_products中的产品门类或产品型号]]
            }
            回答时请严格基于事实，尽可能使用简体中文
        '''%(article_title, section_content, entity_obj_name),
            system_instruction="你是一个工业信息采集员，擅长用json的标准化格式回答用户的提问",
            mode="json",
            model="qwen-turbo",
            temperature=0.05,
            enable_search=True
        )
    # print(ai_response)
    try:
        return json.loads(ai_response)
    except Exception as e:
        print(e)
        return {}

def process_record(record, neo4j_host):
    section_id = record["id"]
    section_content = record["content"]
    article_title = record["title"]
    entity_obj_name = record["entity_obj_name"]
    entity_obj_id = record["entity_obj_id"]
    entity_obj_dict = {entity_obj_name[i]: entity_obj_id[i] for i in range(len(entity_obj_name))}
    
    ai_response = ai_product_recognition(article_title, section_content, entity_obj_name)
    
    if isinstance(ai_response, dict):
        all_products = ai_response.get("all_products", [])
        entity_produce_product = ai_response.get("entity_produce_product", {})
        entity_need_product = ai_response.get("entity_need_product", {})
        analyzation = ai_response.get("analyzation", "")
        
        # 提取共用邏輯為函數
        def handle_products(entity_dict, rel_type):
            for entity, product_list in entity_dict.items():
                if entity in entity_obj_dict:
                    entity_id = entity_obj_dict[entity]
                    for product in product_list:
                        if product in all_products:
                            product_id = neo4j_host.Create_node(
                                label="Product", 
                                attributes={"name": product}
                            )
                            neo4j_host.Crt_rel_by_id(
                                start_node_id=entity_id,
                                end_node_id=product_id,
                                relationship_type=rel_type,
                                rel_attributes={
                                    "reference_section": section_id,
                                    "analysing_process": analyzation
                                }
                            )
        
        handle_products(entity_produce_product, "Produce")
        handle_products(entity_need_product, "Need")

    # 標記處理完成
    neo4j_host.execute_query(
        "MATCH (n:Section) WHERE elementid(n) = $section_id SET n.find_product = true",
        parameters={"section_id": section_id}
    )

while True:
    record_list = neo4j_host.execute_query('''
        MATCH (s:Section) WHERE s.len > 100 and s.find_product is null
        WITH s order by rand() limit 2000
        MATCH (a:Article)<-[:SectionOf]-(s)-[:Mention]->(e:Entity)
        WITH a, s, e
        MATCH (e)-[:FullNameIs]->(eo:EntityObj)-[:SupplyProductTo]-()
        WITH a, s, e, COLLECT(eo)[0] AS single_eo 
        RETURN 
            COLLECT(single_eo.name) AS entity_obj_name,
            COLLECT(elementId(single_eo)) AS entity_obj_id,
            elementid(s) AS id,
            s.content AS content,
            a.title AS title
    ''')
    
    # print(record_list)
    if len(record_list)==0:
        print("没有需要处理的Section 休眠十分钟")
        time.sleep(600)

    # 使用線程池並行處理
    with concurrent.futures.ThreadPoolExecutor(max_workers=24) as executor:  # 可調整worker數量
        futures = [executor.submit(process_record, record, neo4j_host) for record in record_list]
        
        # 可選：添加進度條顯示
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures)):
            try:
                future.result()  # 顯式獲取結果以捕捉異常
            except Exception as e:
                print(f"Error processing record: {e}")
                