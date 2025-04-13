# import API.ai_ask
import API.neo4j_SPLC
neo4j_host=API.neo4j_SPLC.Neo4jClient(driver=API.neo4j_SPLC.local_driver)

# from collections import Counter
from tqdm import tqdm

# from text_process.chunks import text_splitter_zh_en
# spliter=text_splitter_zh_en(zh_max_len=256, en_max_len=512, overlap_ratio=0.25)

from API.ai_ask import get_qwen_embedding

# 2. 使用Qwen的版本

import concurrent.futures
from tqdm import tqdm
import time

# 假设 MAX_WORKERS 是你希望同时运行的最大线程数
MAX_WORKERS = 5

def process_node(record, neo4j_host):
    id = record["id"]
    title = record["title"]
    content = record["content"]
    name = record["name"]
    full_name = record["full_name"]
    
    if isinstance(full_name, str):  # ProductCategpry
        embedding = get_qwen_embedding(text=full_name, dimensions=512)
    elif isinstance(name, str):    # EntityObj
        embedding = get_qwen_embedding(text=name, dimensions=512)
    elif isinstance(content, str):   # Section
        embedding = get_qwen_embedding(text=content, dimensions=512)
    elif isinstance(title, str):   # Article
        embedding = get_qwen_embedding(text=title, dimensions=512)
    else:
        embedding = []
    
    # 更新节点的embedding属性
    neo4j_host.execute_query(                # 这里单独设定了一个Qwen Embedding变量
        "MATCH (n) WHERE elementId(n) = $id SET n.qwen_embedding = $embedding", 
        parameters={"id": id, "embedding": embedding}
    )


with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    while True:
        nodes_to_embed = neo4j_host.execute_query(  # 暂时取消对Section的Embedding
            "MATCH (n: ProductCategory | Product) WHERE n.qwen_embedding IS NULL RETURN elementId(n) AS id, n.full_name as full_name, n.name as name, n.title as title, n.content as content LIMIT 1000"
            # "MATCH (n) WHERE n.qwen_embedding IS NULL RETURN elementId(n) AS id, n.title AS title, n.content AS content LIMIT 1000"
        )
        if len(nodes_to_embed)==0:
            time.sleep(60*15)
            print("嵌入全部完成")
            
        
        # 使用as_completed确保按完成顺序处理
        futures = {executor.submit(process_node, record, neo4j_host): record for record in nodes_to_embed}
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures)):
            try:
                future.result()  # 检查是否有异常抛出
            except Exception as e:
                print(f"Generated an exception: {e}")