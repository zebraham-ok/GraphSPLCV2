from FlagEmbedding import BGEM3FlagModel
import torch
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np

print("CUDA available:", torch.cuda.is_available())
print("CUDA version:", torch.version.cuda)

class EmbedHost:
    def __init__(self, local=False, local_model_path=r"E:\Coding\LLM\BAAI"):
        if local:
            self.model = BGEM3FlagModel(local_model_path,  use_fp16=True)
        else:
            self.model = BGEM3FlagModel('BAAI/bge-m3',  use_fp16=True)
        # Setting use_fp16 to True speeds up computation with a slight performance 
    def encode(self, sentence):
        "句子嵌入"
        return self.model.encode(sentence, 
                            batch_size=12, 
                            max_length=8192, # If you don't need such a long length, you can set a smaller value to speed up the encoding process.
                            )['dense_vecs']
    def smilarity(self, sentence1, sentence2):
        "比较两个句子的相似度，返回一个字典"
        return self.model.compute_score([sentence1,sentence2])
    
    def compute_score(self, sentences):
        "比较两个句子的相似度，返回一个数"
        embeddings = [self.encode(s) for s in sentences]
        similarity = cosine_similarity(np.array(embeddings[0]).reshape(1, -1), 
                                       np.array(embeddings[1]).reshape(1, -1))
        return similarity[0][0]