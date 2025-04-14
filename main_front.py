from Neo4jHost import get_reomte_driver
from main.NER_RE_Entity import ner_re_entity_main
# from main.NER_RE_Product import ner_re_product_main
from main.SQL2Neo4j import sql2neo4j_main
from main.EntityDes import entity_des_main
from main.QwenEmbedding import qwen_embedding_main
from concurrent.futures import ThreadPoolExecutor

neo4j_host=get_reomte_driver()

# 定义主函数
def main_end():
    # 使用线程池并行运行两个函数
    with ThreadPoolExecutor(max_workers=4) as executor:
        future1 = executor.submit(ner_re_entity_main, neo4j_host, 15)
        future2 = executor.submit(entity_des_main, neo4j_host, 2)
        future3 = executor.submit(sql2neo4j_main, neo4j_host, 10)
        future4 = executor.submit(qwen_embedding_main, neo4j_host, 5)
        
        # 等待两个任务完成（可选）
        future1.result()
        future2.result()
        future3.result()
        future4.result()

if __name__ == "__main__":
    main_end()