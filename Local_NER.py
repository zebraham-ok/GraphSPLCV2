import API.ai_ask
import API.neo4j_SPLC
neo4j_host=API.neo4j_SPLC.Neo4jClient(driver=API.neo4j_SPLC.local_driver)

from tqdm import tqdm

from text_process.chunks import text_splitter_zh_en
spliter=text_splitter_zh_en(zh_max_len=256, en_max_len=512, overlap_ratio=0.25)

import API.SQL_SPLC
sql_host=API.SQL_SPLC.generate_sql_host(database="splc")

import torch

print("CUDA available:", torch.cuda.is_available())
print("CUDA version:", torch.version.cuda)

from text_process.ner import NerHost

from concurrent.futures import ThreadPoolExecutor, as_completed


ner_host=NerHost()

# 假设 batch_size 和其他变量已经在外部定义好
batch_size = 100

MAX_WORKERS = 15

def process_record(record):
    content = record["content"]
    section_id = record["id"]
    if isinstance(content, str):
        raw_result, merged_result = ner_host.ner(content)
        if merged_result:
            for entity_info_dict in merged_result:
                entity = entity_info_dict["word"]
                entity_node_id = neo4j_host.Create_node(label="Entity", attributes={"name": entity, "type": entity_info_dict["entity"]})
                neo4j_host.Crt_rel_by_id(start_node_id=section_id, end_node_id=entity_node_id, relationship_type="Mention", rel_attributes={})
            neo4j_host.execute_query("match (n) where elementid(n)=$id set n.ner=True", parameters={"id": section_id})
        else:
            neo4j_host.execute_query("match (n) where elementid(n)=$id set n.ner=False", parameters={"id": section_id})

while True:
    without_ner_records = neo4j_host.execute_query(query=f'''
        match (n:Section) where n.ner is null
        return n.content as content, elementid(n) as id
        limit {batch_size}''')
    
    # 使用 ThreadPoolExecutor 创建一个线程池
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:  # 调整 max_workers 根据你的需求
        futures = {executor.submit(process_record, record): record for record in without_ner_records}
        
        # 等待所有线程完成
        for future in tqdm(as_completed(futures), total=len(futures)):
            try:
                future.result()  # 获取线程的结果或异常
            except Exception as e:
                print(f"Generated an exception: {e}")
                
    if len(without_ner_records)==0:
        print("NER任务全部完成")
                
                