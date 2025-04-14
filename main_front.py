from Neo4jHost import neo4j_host
from main.NER_RE_Entity import ner_re_entity_main
# from main.NER_RE_Product import ner_re_product_main
from main.SQL2Neo4j import sql2neo4j_main
from main.EntityDes import entity_des_main
from main.QwenEmbedding import qwen_embedding_main

# 定义要启动的脚本列表
# scripts = ["SQL2Neo4j.py", "NER_RE_Entity.py", "NER_RE_Product.py", "EntityDes.py","QwenEmbedding.py"]
# scripts = ["NER_RE_Entity.py", "NER_RE_Product.py", "EntityDes.py","QwenEmbedding.py"]

from concurrent.futures import ThreadPoolExecutor

# 定义主函数
def main_end():
    # 使用线程池并行运行两个函数
    with ThreadPoolExecutor(max_workers=4) as executor:
        future1 = executor.submit(ner_re_entity_main, neo4j_host, 15)
        future2 = executor.submit(entity_des_main, neo4j_host, 2)
        future3 = executor.submit(sql2neo4j_main, neo4j_host, 10)
        future3 = executor.submit(qwen_embedding_main, neo4j_host, 5)
        
        # 等待两个任务完成（可选）
        future1.result()
        future2.result()
        future3.result()

if __name__ == "__main__":
    main_end()