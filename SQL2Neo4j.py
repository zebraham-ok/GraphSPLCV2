import API.ai_ask
import API.neo4j_SLPC
neo4j_host=API.neo4j_SLPC.Neo4jClient(driver=API.neo4j_SLPC.local_driver)

from tqdm import tqdm

from text_process.chunks import text_splitter_zh_en
spliter=text_splitter_zh_en(zh_max_len=512, en_max_len=1024, overlap_ratio=0.15)

import API.SQL_SPLC
sql_host=API.SQL_SPLC.generate_sql_host(database="splc")
# local_sql_host=API.SQL_SPLC.generate_sql_host(database="splc", sql_host_name="localhost", sql_secret="mysql", user="root", sql_port=3306)

# 这个是从的新的crawler_main中执行导入
import concurrent.futures

def process_article_record(record, neo4j_host: API.neo4j_SLPC.Neo4jClient, spliter):
    "引入了判别模型"
    sql_us_id,item,title,content,language,page_date = record
    if isinstance(content, str):
        # article_embedding=API.ai_ask.get_qwen_embedding(text=title, dimensions=512)
        # probas, article_useful=predict_text(embedding=article_embedding, mode="Article", threshold=0.4)
        # if probas:
        #     sql_host._execute_query(query="update crawler_main where US_id=:sql_us_id set useful=:useful", params={"sql_us_id":sql_us_id, "useful": probas})

        # 先进行文本预处理，避免过长的目录、空格占据空间
        content=content.replace("....", "").replace("····", "").replace("     ","")
        if isinstance(item, str):  # 对pdf进行去换行操作。
            if item.endswith("pdf") or item.endswith("PDF") or item.endswith("Pdf"):
                content.replace("\n\n","    ")  # 两个才是真换行
                content.replace("\n","")
            
        article_data = {
            "sqlId": sql_us_id,
            "title": title,
            "url": item,
            "pageTime":page_date,
            "language":language,
            # "useful_prob": probas,
            # "qwen_embedding": article_embedding
        }
            
        # 创建文章对象
        article_id=neo4j_host.Create_node(label="Article",attributes=article_data, set_date=True)
        
        sections_list = spliter.split_str(text=content)
        for i in range(len(sections_list)):
            sec_content=sections_list[i]
            if sec_content and isinstance(sec_content, str) and len(sec_content)<4000:
                # section_embedding=API.ai_ask.get_qwen_embedding(text=sec_content, dimensions=512)
                # probas, section_useful=predict_text(embedding=section_embedding, mode="Section", threshold=0.2)
                section_data = {
                    "content": sec_content,
                    "position": i,
                    "len": len(sec_content),
                    # "useful_prob": probas,
                    # "embedding":section_embedding
                    # 这里其实应该引入向量化
                }
                # 创建section对象
                section_id = neo4j_host.Create_node("Section", attributes=section_data)
                neo4j_host.Crt_rel_by_id(start_node_id=section_id,end_node_id=article_id,relationship_type="SectionOf",rel_attributes={})
        
    # 将成功录入系统的条目，同时在SQL数据库中进行标记
    sql_host._execute_query(query='''
        UPDATE `crawler_main`
        SET `load_time` = CURRENT_TIMESTAMP
        WHERE `US_id` = :sql_us_id; ''',
        params={"sql_us_id":sql_us_id})

while True:
    # print(f"The offset now is {offset}")
    # 结合文章的长度、prob来综合判断是否要导入这个文章
    result=sql_host._execute_query(query="select US_id,item,title,content,language,page_date from crawler_main where load_time is null and content is not null and content_len<20000 order by useful desc limit 1000")
    result_list=result.fetchall()
    if len(result_list)==0:
        break

    with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
        list(tqdm(executor.map(lambda record: process_article_record(record, neo4j_host, spliter), result_list), total=len(result_list)))
    # break