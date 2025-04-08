import API.SQL_SPLC
sql_host=API.SQL_SPLC.generate_sql_host(database="splc")

# from collections import Counter
from tqdm import tqdm

# from text_process.chunks import text_splitter_zh_en
# spliter=text_splitter_zh_en(zh_max_len=256, en_max_len=512, overlap_ratio=0.25)

from API.ai_ask import get_qwen_embedding


# 导入判别模型
import torch
from procedures.ArticleSectionRec01 import SimpleClassifier
# device = torch.device("cpu")
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
article_model = SimpleClassifier(input_dim=512).to(device)
article_model.load_state_dict(torch.load(r"model\article_discriminate_model.pth", map_location=device, weights_only=True))
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
    
    with torch.no_grad():
        embedding_tensor = torch.tensor(embedding, dtype=torch.float32).unsqueeze(0)  # 添加 batch 维度
        probas = torch.sigmoid(model(embedding_tensor.to(device))).cpu().numpy().item()
        predictions = (probas > threshold)
        
    # 返回一个01变量判定它是否是我们想要的
    return probas, predictions

while True:
    result=sql_host._execute_query(query="select US_id, title from crawler_main where load_time is null and useful is null and content is not null and title is not null limit 1000")
    result_list=result.fetchall()
    if not result_list:
        break
    for record in tqdm(result_list):
        us_id, title=record
        if isinstance(title, str):
            embedding=get_qwen_embedding(title)
            if embedding:
                proba, prediction=predict_text(embedding=embedding, mode="Article", threshold=0.4)
                sql_host._execute_query(query="update crawler_main set useful=:useful where US_id=:us_id", params={"useful": proba, "us_id": us_id})