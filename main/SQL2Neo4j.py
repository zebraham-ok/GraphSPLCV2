# import API.ai_ask
import API.neo4j_SPLC
# neo4j_host=API.neo4j_SPLC.Neo4jClient(driver=API.neo4j_SPLC.local_driver)
import time
from tqdm import tqdm

from text_process.chunks import text_splitter_zh_en
spliter=text_splitter_zh_en(zh_max_len=512, en_max_len=1024, overlap_ratio=0.15)

import API.SQL_SPLC
sql_host=API.SQL_SPLC.generate_sql_host(database="splc")
# local_sql_host=API.SQL_SPLC.generate_sql_host(database="splc", sql_host_name="localhost", sql_secret="mysql", user="root", sql_port=3306)

# 这个是从的新的crawler_main中执行导入
import concurrent.futures

CREATE_ARTICLE_SECTIONS_QUERY='''
    CREATE (a:Article {title: $article.title})
    SET 
        a.url = $article.url,
        a.pageTime = $article.pageTime,
        a.language = $article.language,
        a.createDate = datetime()

    WITH a, $sections AS sections
    UNWIND sections AS sec
    CREATE (s:Section {content: sec.content})
    SET 
        s.position = sec.position,
        s.len = sec.len,
        s.createDate = datetime()
    MERGE (s)-[:SectionOf]->(a)

    RETURN count(DISTINCT s) AS section_count, a.title AS article_title
'''

def process_article_record(record, neo4j_host: API.neo4j_SPLC.Neo4jClient, spliter):
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
        sections_content_list = spliter.split_str(text=content)
        section_list_of_dict=[]
        for i in range(len(sections_content_list)):
            sec_content=sections_content_list[i]
            if sec_content and isinstance(sec_content, str) and len(sec_content)<4000:
                section_data = {
                    "content": sec_content,
                    "position": i,
                    "len": len(sec_content),
                    # "useful_prob": probas,
                    # "embedding":section_embedding
                    # 这里其实应该引入向量化
                }
                section_list_of_dict.append(section_data)
                
        neo4j_host.execute_query(CREATE_ARTICLE_SECTIONS_QUERY, parameters={"article": article_data, "sections": section_list_of_dict})
        
    # 将成功录入系统的条目，同时在SQL数据库中进行标记
    sql_host._execute_query(query='''
        UPDATE `crawler_main`
        SET `load_time` = CURRENT_TIMESTAMP
        WHERE `US_id` = :sql_us_id; ''',
        params={"sql_us_id":sql_us_id})

def sql2neo4j_main(neo4j_host=None, max_workers=15):
    "将sql中的信息导入neo4j当中"
    if not neo4j_host:
        neo4j_host=API.neo4j_SPLC.Neo4jClient(driver=API.neo4j_SPLC.local_driver)
    while True:
        # print(f"The offset now is {offset}")
        # 结合文章的长度、prob来综合判断是否要导入这个文章
        # result=sql_host._execute_query(query="""
        #         select US_id, item, title, content, language, page_date from crawler_main
        #         where load_time is null and content is not null and content_len<20000
        #         order by useful desc limit 1000""")
        result_list=sql_host._execute_query(query="""
                select US_id, item, title, content, language, page_date from crawler_main
                where load_time is null and content is not null and content_len<50000 and LEFT(page_date, 4)>"2019"
                order by useful desc limit 1000""") # 暂时增加从2020年之后数据的导入
        if len(result_list)==0:
            print("没有要导入的章节了")
            time.sleep(600)

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            list(tqdm(executor.map(lambda record: process_article_record(record, neo4j_host, spliter), result_list), total=len(result_list), desc="文本导入Neo4j"))

if __name__=="__main__":
    sql2neo4j_main(neo4j_host=None, max_workers=15)