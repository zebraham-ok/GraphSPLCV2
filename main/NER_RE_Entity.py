"使用大语言模型抽取企业实体和供货关系的信息"
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
# import json
import API.ai_ask
import API.neo4j_SPLC
from procedures.check_rubbish import check_rubbish
from text_process.find_json import get_dict_from_str
from text_process.timeStamp import parse_date

# neo4j_host=API.neo4j_SPLC.Neo4jClient(driver=API.neo4j_SPLC.local_driver)

from text_process.chunks import text_splitter_zh_en
spliter=text_splitter_zh_en(zh_max_len=256, en_max_len=512, overlap_ratio=0.2)
    
def ai_entity_recognition(article_title, content):
    "用大模型直接确认原文中的实体，分析这些实体是否是具体的，并给出它们的正式中文名称"
    # 这一步需要的中国特有知识较多，因此使用qwen更加准确
    ai_response=API.ai_ask.ask_qwen_with_gpt_backup(prompt_text='''
            请查看如下文本片段，严格依照原文文本内容，按照如下步骤，结合你检索到的知识完成任务：
            {"title": %s, "section_content": %s}
            1. 请在文中找出所有的组织实体，在all_entities一栏以列表的形式返回一个list[str]，作为结果的第一项
            2. 请判断这些组织实体在原文中只是一个抽象的概念或者一类组织实体的集合（不符合条件），还是能查到工商登记或者机构官网的那种具体的对象（符合条件）。请在specific_entities一栏，列出所有符合条件的具体组织实体，以list[str]的形式汇总，作为结果的第二项
            3. 请结合你的知识和检索的信息，返回“以上文中每一个具体的组织实体在原文中的名称作为key，以该具体组织实体的正式全称作为value”的字典，称为full_cn_name_dict，作为结果的第三项。value尽可能使用中文。如果原文中出现的就是组织实体的正式全称，不变即可。
            4. 如果该段落中有代词代指具体的组织实体（如“本公司”、“该企业”等），请在pronoun_entity一栏列出，用一个字典形式说明其在原文中指代对象的正全称，作为结果的第四项。
            请以json格式回答问题，格式如下所示：
            {
                "all_entities": list[str],
                "specific_entities": list[str],
                "full_cn_name_dict": dict[原文中的组织实体名: 正式全称],
                "pronoun_entity": dict[代词: 指代对象的正式中文全称]
            }
            注意：
            （1）这里的“组织实体”仅指企业、研究机构、政府机构等组织，必须是具体的名称；
            （2）回答时请严格基于事实，不要臆想；
            （3）请不要在json中使用嵌套引号和注释！
        '''%(article_title, content),
        system_instruction="你是一个商业信息采集员，擅长用json的标准化格式回答用户的提问",
        mode="json",
        model="qwen-turbo",
        temperature=0.05,
        enable_search=True
        )
    if not ai_response:
        print("ai_entity_recognition not responded by both qwen and gpt")
        return {}
    try:
        ai_list=get_dict_from_str(ai_response)
        if ai_list:
            return ai_list[0]
        else:
            print("ai_entity_recognition is responded but cannot be turned into dict: ", ai_response, "\n", "the original section content is like this: ", article_title+": "+content)
            return {}
    except Exception as e:
        print(e)
        return {}
    
# allowed_relation_types={"SupplyProductTo": "一个公司（前者）给另一个公司（后者）供货，或提供某种服务", "WinBidFor": "一个公司（前者）赢得了另一个公司/机构（后者）的招标", "SubsidiaryOf": "一个组织（前者）是另一个组织（后者）的分支"}
# allowed_entity_types={"Company": "商业性的企业", "Government": "中央政府、地方政府或联合国机构", "Academic": "大学或研究所", "NGO": "非营利性组织和社团", "Others": "其它类型的组织，或非组织的、意义不明的字符串"}

ALLOWED_ENTITY_TYPES_FOR_ORG={"Company", "Factory", "Government", "MiningSite", "Academic", 'Media', "NGO", "Others", "Factory"}
ALLOWED_RELATION_TYPES_FOR_ORG={"SupplyProductTo", "OwnFactory", "OwnMiningSite", "PartnerOf", "OfferFianceService", "WinBidFor", "SubsidiaryOf", "GrantTechTo","IsSalesAgentOf"}
# 增加了一个销售代理关系，以与供货关系相区别


def ai_relation_extraction_ORG_gpt(article_title, content, formal_entity_list,                              
                                   allowed_entity_types=ALLOWED_ENTITY_TYPES_FOR_ORG, 
                                   allowed_relation_types=ALLOWED_RELATION_TYPES_FOR_ORG, model="gpt-4o"):
    "用大模型检查一个content当中各个机构之间是否有给定类型的关系"
    # 这个提示词可能会导致，大模型忽略了一些中断信息，把它当做是不具有供应关系的体现
    ai_response=API.ai_ask.ask_gpt(prompt_text='''
            请查看如下文本片段，严格依照原文文本内容，按照如下步骤，结合你检索到的知识完成任务：
            {"title": %s, "section_content": %s, "entity_list": %s, "allowed_entity_types": %s, "allowed_relation_types": %s}
            1. 请查看entity_list当中的组织实体，判断他们属于allowed_entity_types中的什么类型的entity_type，仅能从这些类型中选择。判断完成后，在entity_type_dict一栏以字典的形式返回一个dict，给出结果的第一项
            2. 请判断这些组织实体之间是否存在allowed_entity_types中的关系，如果存在，在relationship_list一栏中以list[dict]的形式，给出结果的第二项。有多少条关系就写多少个，如果没有，则返回一个空列表。请注意识别关系的方向，并在analysing_process一项中写清楚你的推导过程。
            请以json格式回答问题，格式如下所示：
            {
                "entity_type_dict": dict[entity: entity_type],
                "relationship_list": list[dict["analysing_process": str, "source":entity, "relation_type":relation_type, "target": entity]]
            }
            注意:
            （1）你仅需要识别allowed_relation_types和allowed_entity_types中的信息类型
            （2）金融服务属于OfferFianceService，实际生产制造中的上下游之间提供货物，或生产性的服务才属于SupplyProductTo，基于技术专利等知识产权的授权属于GrantTechTo
            （3）对于公司间中断供货、终止客户关系等现象，也要以SupplyProductTo的形式进行记录，毕竟这表明该关系曾经存在
            （4）回答时请严格基于事实，不要臆想
            （5）请不要在json中使用嵌套引号和注释！
        '''%(article_title, content, formal_entity_list, allowed_entity_types, allowed_relation_types),
        system_instruction="你是一个商业信息采集员，擅长用json的标准化格式回答用户的提问",
        mode="json",
        # model="qwen-plus",
        model=model,
        temperature=0,
        # enable_search=True
        )
    if not ai_response:
        print("ai_relation_extraction_ORG_gpt not responded by both qwen and gpt")
        return {}
    try:
        ai_list=get_dict_from_str(ai_response)
        if ai_list:
            return ai_list[0]
        else:
            print("ai_relation_extraction_ORG_gpt is responded but cannot be turned into dict: ", ai_response)
            return {}
    except Exception as e:
        print(e)
        return {}
    
def ai_relation_extraction_ORG(article_title, content, formal_entity_list, allowed_entity_types=ALLOWED_ENTITY_TYPES_FOR_ORG, allowed_relation_types=ALLOWED_RELATION_TYPES_FOR_ORG, model="qwen3-max", enable_thinking=False):
    "用大模型检查一个content当中各个机构之间是否有给定类型的关系"
    ai_response=API.ai_ask.ask_qwen_with_gpt_backup(prompt_text='''
            请查看如下文本片段，严格依照原文文本内容，按照如下步骤，结合你检索到的知识完成任务：
            {"title": %s, "section_content": %s, "entity_list": %s, "allowed_entity_types": %s, "allowed_relation_types": %s}
            1. 请查看entity_list当中的组织实体，判断他们属于allowed_entity_types中的什么类型的entity_type，仅能从这些类型中选择。判断完成后，在entity_type_dict一栏以字典的形式返回一个dict，给出结果的第一项
            2. 请判断这些组织实体之间是否存在allowed_entity_types中的关系，如果存在，在relationship_list一栏中以list[dict]的形式，给出结果的第二项。有多少条关系就写多少个，如果没有，则返回一个空列表。请注意识别关系的方向（例如，在SupplyProductTo中，供应商是source，客户是target），并在analysing_process一项中写清楚你的推导过程。
            请以json格式回答问题，格式如下所示：
            {
                "entity_type_dict": dict[entity: entity_type],
                "relationship_list": list[dict["analysing_process": str, "source":entity, "relation_type":relation_type, "target": entity]]
            }
            注意：
            （1）你仅需要识别allowed_relation_types和allowed_entity_types中的信息类型
            （2）金融服务属于OfferFianceService，实际生产制造中的上下游之间提供货物，或生产性的服务才属于SupplyProductTo，基于技术专利等知识产权的授权属于GrantTechTo
            （3）Factory和MiningSite都是从属于公司的，在生成它们的正式名称时应尽量带上其公司名
            （4）回答时请严格基于事实，不要臆想。
            （5）请不要在json中使用嵌套引号和注释！
        '''%(article_title, content, formal_entity_list, allowed_entity_types, allowed_relation_types),
        system_instruction="你是一个商业信息采集员，擅长用json的标准化格式回答用户的提问",
        mode="json",
        # model="llama-4-maverick-17b-128e-instruct",
        # model="deepseek-r1-distill-llama-8b",
        model=model,
        temperature=0,
        # enable_search=True
        enable_thinking=enable_thinking
        )
    if not ai_response:
        print("ai_relation_extraction_ORG not responded by both qwen and gpt")
        return {}
    try:
        ai_list=get_dict_from_str(ai_response)
        if ai_list:
            return ai_list[0]
        else:
            print("ai_relation_extraction_ORG is responded but cannot be turned into dict: ", ai_response, "\n", "the original section content is like this: ", article_title+": "+content)
            return {}
    except Exception as e:
        print(e)
        return {}

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from datetime import date

# 配置参数
BATCH_SIZE = 100
WORKER_THREADS = 14
EMPTY_SLEEP = 600

# Cypher查询模板
SET_ENTITY_QUERY = """
MATCH (startNode) WHERE elementId(startNode) = $startNodeId 
MATCH (endNode) WHERE elementId(endNode) = $endNodeId
SET endNode.qwen = true
CREATE (startNode)-[rel:Mention]->(endNode)
SET rel.qwen = true
"""

SET_ENTITY_TYPE_QUERY = """
MATCH (n)
WHERE elementId(n) = $id
SET n:{type}
"""

MARK_SECTION_PROCESSED = """
MATCH (s:Section) 
WHERE elementId(s) = $id 
SET s.find_entity = true
"""

class SectionProcessor:
    def __init__(self, neo4j_host: API.neo4j_SPLC.Neo4jClient, max_worker=WORKER_THREADS):
        self.neo4j_host = neo4j_host
        self.executor = ThreadPoolExecutor(max_workers=max_worker)

    def fetch_unprocessed_sections(self):
        """获取未处理的章节数据"""
        # result = self.neo4j_host.execute_query('''
        #     MATCH (s:Section)-[:SectionOf]->(a:Article)
        #     WHERE s.find_entity IS NULL
        #     RETURN elementId(s) AS id, s.content AS content, a.title AS title, a.pageTime as pageTime
        #     ORDER BY rand()
        #     LIMIT $batch_size''',
        #     parameters={"batch_size": BATCH_SIZE}
        # )
        result = self.neo4j_host.execute_query('''
            MATCH (s:Section)-[:SectionOf]->(a:Article)
            WHERE s.find_entity IS NULL
            RETURN elementId(s) AS id, s.content AS content, a.title AS title, a.pageTime as pageTime, a.url as url
            order by rand()
            LIMIT $batch_size''',
            parameters={"batch_size": BATCH_SIZE}
        )
        return result

    def process_entity_types(self, type_mapping, entity_map, full_name_dict):
        """处理实体类型标签"""
        for entity_name, entity_type in type_mapping.items():
            # 确定正式实体名称
            canonical_name = full_name_dict.get(entity_name, entity_name)
            
            # 获取实体ID
            entity_id = entity_map.get(canonical_name)
            if not entity_id or entity_type not in ALLOWED_ENTITY_TYPES_FOR_ORG:
                continue
                
            # 执行类型标签更新
            self.neo4j_host.execute_query(
                SET_ENTITY_TYPE_QUERY.format(type=entity_type),
                parameters={"id": entity_id}
            )

    def create_entity_structure(self, section_id, alias, canonical, entity_map):
        """创建实体节点和关系结构"""
        # 创建基础实体节点
        alias_rubbish_label=check_rubbish(alias)
        canonical_rubbish_label=check_rubbish(canonical)
        
        base_id = self.neo4j_host.Create_node(
        # Entity在文中的merge或许也可以为未来识别出同名词提供参考
            "Entity", {"name": alias, "qwen": True, "rubbish": alias_rubbish_label}, merge=True
        )
        
        # 创建规范实体节点
        canonical_id = self.neo4j_host.Create_node(
            "EntityObj", {"name": canonical, "qwen": True, "rubbish": canonical_rubbish_label}, merge=True
        )
        entity_map[canonical] = canonical_id
        
        # 建立关联关系
        self.neo4j_host.execute_query(
            SET_ENTITY_QUERY,
            parameters={"startNodeId": section_id, "endNodeId": base_id}
        )
        result=self.neo4j_host.Crt_rel_by_id(base_id, canonical_id, "FullNameIs", start_node_label="Entity", end_node_label="EntityObj", set_date=False)
        if "error" in result:
            print(result)
        
        return canonical_id

    def process_entities(self, section_id, entity_info):
        """处理实体识别结果"""
        entity_map = {}
        full_name_dict = entity_info.get("full_cn_name_dict", {})
        
        # 处理主要实体
        for alias, canonical in full_name_dict.items():
            self.create_entity_structure(section_id, alias, canonical, entity_map)
            
        # 处理指代实体
        for pronoun, true_entity in entity_info.get("pronoun_entity", {}).items():
            if true_entity not in full_name_dict.values():
                canonical_id = self.create_entity_structure(section_id, pronoun, true_entity, entity_map)
                full_name_dict[pronoun] = true_entity
                
        return entity_map, full_name_dict

    def process_relationships(self, relationships, entity_map, section_id, original_content, url, time_stamp="", model=None):
        """处理关系抽取结果"""
        for rel in relationships:
            source_id = entity_map.get(rel["source"])
            target_id = entity_map.get(rel["target"])
            
            if not all([source_id, target_id]):
                continue
                
            rel_attr={
                        "reference_section": section_id,
                        "analysing_process": rel.get("analysing_process", ""),
                        "original_content": original_content,
                        "url": url,
                        "updated_at": date.today(),
                        "qwen": True
                    }
            # 方便统计对每一个连边负有责任的模型
            if model:
                rel_attr["model"]=model
            
            if time_stamp:
                rel_attr["time"]=time_stamp
            
            relation_type=rel["relation_type"]
            if relation_type in ALLOWED_RELATION_TYPES_FOR_ORG:
                self.neo4j_host.Crt_rel_by_id(
                    source_id, 
                    target_id, 
                    relationship_type=relation_type,
                    rel_attributes=rel_attr,
                    start_node_label="EntityObj",
                    end_node_label="EntityObj",
                    set_date=True
                )
                
                # 对不是Company的工厂、矿山进行更名，以确保其不会重名（但这个可能会导致重复创建节点，所以暂时还是不要运行了），如果只是给Factory或MiningSite增加一个属性，有不足以确保他们在CreateNode方法中不会被其它企业的同名工厂给merge了。
                # if relation_type in ("OwnFactory", "OwnMiningSite"):
                #     self.neo4j_host.execute_query(query='''
                #         match (a), (c)
                #         where elementid(a)=$asset_id and elementid(c)=$company_id
                #         WITH a, c
                #         WHERE NOT 'Company' IN labels(a)
                #         set a.name = a.name + '-' + c.name
                #     ''', parameters = {"asset_id": target_id, "company_id": source_id})

    def process_single_section(self, record, model):
        """单个章节的处理流水线"""
        try:
            section_id = record["id"]
            content = record["content"]
            title = record["title"]
            pageTime = record["pageTime"]
            url = record["url"]
            
            time_stamp=""
            if pageTime:
                date_dict=parse_date(pageTime)
                if "date" in date_dict:
                    time_stamp=date_dict["date"]
                    
            # 实体识别阶段
            entity_result = ai_entity_recognition(title, content)
            if not entity_result:
                raise "Not a Dict"
            if entity_result.get("full_cn_name_dict"):                
                # print(entity_result)
                # 创建实体结构
                entity_map, full_name_dict = self.process_entities(section_id, entity_result)
                # print(full_name_dict)
                # 关系抽取阶段
                relation_result = ai_relation_extraction_ORG(
                    content, 
                    title, 
                    formal_entity_list=list(entity_map.keys()),
                    model=model
                )
                
                # print("relation result: ", relation_result)
                if relation_result and isinstance(relation_result, dict):
                    main_result = relation_result
                    # print(main_result)
                    # 处理实体类型
                    if "entity_type_dict" in main_result:
                        self.process_entity_types(
                            main_result["entity_type_dict"],
                            entity_map,
                            full_name_dict
                        )
                    
                    # 处理关系列表
                    if "relationship_list" in main_result:
                        self.process_relationships(
                            main_result["relationship_list"],
                            entity_map,
                            section_id,
                            "《"+title+"》: "+content if title else content,
                            url,
                            time_stamp=time_stamp,
                            model=model
                        )
                
                # 标记章节为已处理
            self.neo4j_host.execute_query(
                    MARK_SECTION_PROCESSED,
                    parameters={"id": section_id}
                    )
            
        except Exception as e:
            print(f"处理章节 {section_id} 时发生错误: {str(e)}")
            self.neo4j_host.execute_query("match (n) where elementid(n)=$id set n.find_entity=false", parameters={"id": section_id})
            # 之后统一对错误记录进行检查

    def run_processing_loop(self, model):
        """主处理循环"""
        while True:
            sections = self.fetch_unprocessed_sections()
            # print(sections)
            if not sections:
                print(f"没有未处理的章节，休眠 {EMPTY_SLEEP} 秒...")
                time.sleep(EMPTY_SLEEP)
                continue
                
            # 提交并行任务
            futures = []
            for section in sections:
                future = self.executor.submit(
                    self.process_single_section, 
                    section,
                    model
                )
                futures.append(future)
                # break
            
            # 使用进度条监控
            with tqdm(total=len(futures), desc="实体关系抽取") as pbar:
                for _ in as_completed(futures):
                    pbar.update(1)

def ner_re_entity_main(neo4j_host=None, max_worker=10,model="qwen3-32b"):
    "进行初步实体关系抽取"
    if neo4j_host:
        processor = SectionProcessor(neo4j_host, max_worker=max_worker)
    else:
        processor = SectionProcessor(API.neo4j_SPLC.Neo4jClient(driver=API.neo4j_SPLC.local_driver), max_worker=max_worker)
    processor.run_processing_loop(model)

# 使用示例
if __name__ == "__main__":
    processor = SectionProcessor(neo4j_host=API.neo4j_SPLC.Neo4jClient(driver=API.neo4j_SPLC.local_driver))
    processor.run_processing_loop(model="qwen3-32b")