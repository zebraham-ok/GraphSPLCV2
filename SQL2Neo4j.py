import API.ai_ask
import API.neo4j_SLPC
neo4j_host=API.neo4j_SLPC.Neo4jClient(driver=API.neo4j_SLPC.local_driver)

from tqdm import tqdm

from text_process.chunks import text_splitter_zh_en
spliter=text_splitter_zh_en(zh_max_len=512, en_max_len=1024, overlap_ratio=0.15)

import API.SQL_SPLC
sql_host=API.SQL_SPLC.generate_sql_host(database="splc")
local_sql_host=API.SQL_SPLC.generate_sql_host(database="splc", sql_host_name="localhost", sql_secret="mysql", user="root", sql_port=3306)

# 这个是从的新的crawler_main中执行导入
import concurrent.futures

def process_article_record(record, neo4j_host: API.neo4j_SLPC.Neo4jClient, spliter):
    sql_us_id,item,title,content,language,page_date = record
    
    # 先进行文本预处理
    if isinstance(content, str):   # 避免过长的目录、空格占据空间
        content=content.replace("....", "").replace("     ","")
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
    }
    
    # 查看是否有title再进行Embedding，否则None会导致报错（由于太慢暂时停掉了）
    # if title and isinstance(title, str):
    #     article_data["embedding"]=embed_host.encode(title)
        
    # 创建文章对象
    article_id=neo4j_host.Create_node(label="Article",attributes=article_data, set_date=True)
    
    sections_list = spliter.split_str(text=content)
    for i in range(len(sections_list)):
        content=sections_list[i]
        if content and isinstance(content, str) and len(content)<4000:
            section_data = {
                "content": content,
                "position": i,
                "len": len(content)
                # "embedding":embed_host.encode(content)
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
    result=sql_host._execute_query(query="select US_id,item,title,content,language,page_date from crawler_main where load_time is null and content is not null limit 1000")
    result_list=result.fetchall()
    if len(result_list)==0:
        break

    with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
        list(tqdm(executor.map(lambda record: process_article_record(record, neo4j_host, spliter), result_list), total=len(result_list)))
    # break