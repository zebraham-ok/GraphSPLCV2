import API.SQL_SPLC
import API.Mongo_SPLC
# from collections import Counter
from tqdm import tqdm

# from text_process.chunks import text_splitter_zh_en
# spliter=text_splitter_zh_en(zh_max_len=256, en_max_len=512, overlap_ratio=0.25)

from API.ai_ask import get_qwen_embedding
from concurrent.futures import ThreadPoolExecutor, as_completed

# 导入判别模型
import torch
# from procedures.ArticleSectionRec01 import SimpleClassifier
from procedures.ArticleDiscriminate import SimpleClassifier
# device = torch.device("cpu")
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
article_model = SimpleClassifier(input_dim=512).to(device)
article_model.load_state_dict(torch.load(r"model\des_w2_rc70.pth", map_location=device, weights_only=True))
article_model.eval()

# section_model = SimpleClassifier(input_dim=512).to(device)
# section_model.load_state_dict(torch.load(r"model\section_discriminate_model.pth", map_location=device, weights_only=True))
# section_model.eval()
section_model=None

def predict_text(embedding, mode, threshold=0.5):
    if mode=="Article":
        model=article_model
    elif mode=="Section":
        model=section_model
    else:
        print("invalid mode name")
        return None, None
    
    probas=None
    predictions=None
    
    with torch.no_grad():
        embedding_tensor = torch.tensor(embedding, dtype=torch.float32).unsqueeze(0)  # 添加 batch 维度
        try:
            probas = torch.sigmoid(model(embedding_tensor.to(device))).cpu().numpy().item()
            predictions = (probas > threshold)
        except Exception as e:
            print(f"Wrong size of tensor: {embedding_tensor}")
        
    # 返回一个01变量判定它是否是我们想要的
    return probas, predictions

collection="article_embedding"
def process_record(record, mongo:API.Mongo_SPLC.MongoDBManager):
    """
    单条记录的处理逻辑
    """
    us_id, title, des = record
    mongo_record=mongo.find(filter_query={"sqlId": us_id}, collection=collection)
    if mongo_record:
        embedding=mongo_record[0]["qwen_embedding"]
        print(f"Using Cached Vector of {title}")
    else:
        if isinstance(title, str) and isinstance(des, str):
            embedding = get_qwen_embedding("《" + title + "》：" + des)
        elif isinstance(title, str):
            embedding = get_qwen_embedding(title)
        elif isinstance(des, str):
            embedding = get_qwen_embedding(des)
        else:
            return None  # 如果 title 和 des 都不是字符串，跳过该记录
    if embedding and (title or des):
        mongo.save(data={"sqlId": us_id, "title": title, "description": des, "qwen_embedding": embedding}, collection=collection)

    proba, prediction = predict_text(embedding=embedding, mode="Article", threshold=0.4)
    return us_id, proba


def update_records_multithreaded(result_list, sql_host, max_workers=3, mongo=None):
    """
    使用多线程批量更新数据库记录
    
    参数说明：
    result_list - 待处理的记录列表
    sql_host - 数据库连接对象
    max_workers - 最大线程数
    """
    # 创建线程池
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交任务到线程池
        futures = [executor.submit(process_record, record, mongo) for record in result_list]

        # 使用 tqdm 显示进度条
        updates = []
        for future in tqdm(as_completed(futures), total=len(result_list), desc="Processing records"):
            result = future.result()
            if result:  # 如果返回了有效结果
                us_id, proba = result
                updates.append((us_id, proba))

        # 批量更新数据库
        print(f"Updating {len(updates)} records in the database...")
        for us_id, proba in tqdm(updates, desc="Updating database"):
            sql_host._execute_query(
                query="UPDATE crawler_main SET useful=:useful WHERE US_id=:us_id",
                params={"useful": proba, "us_id": us_id}
            )

if __name__=='__main__':
    sql_host=API.SQL_SPLC.generate_sql_host(database="splc")
    
    save_vectors=True
    if save_vectors:
        mongo_host=API.Mongo_SPLC.MongoDBManager()
    else:
        mongo_host=None
    
    while True:
        result_list=sql_host._execute_query(query="select US_id, title, des from crawler_main where load_time is null and useful is null and content is not null and (title is not null or des<>'') limit 500")
        if not result_list:
            print("All Completed")
            break
        
        update_records_multithreaded(result_list, sql_host, max_workers=3, mongo=mongo_host)
        
        # for record in tqdm(result_list):
        #     us_id, title, des = record
        #     if isinstance(title, str) and isinstance(des, str):
        #         embedding=get_qwen_embedding("《"+title+"》："+des)
        #     elif isinstance(title, str):
        #         embedding=get_qwen_embedding(title)
        #     elif isinstance(des, str):
        #         embedding=get_qwen_embedding(des)
        #     else:
        #         continue
        #     proba, prediction=predict_text(embedding=embedding, mode="Article", threshold=0.4)
        #     sql_host._execute_query(query="update crawler_main set useful=:useful where US_id=:us_id", params={"useful": proba, "us_id": us_id})