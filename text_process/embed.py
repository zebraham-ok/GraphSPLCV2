from sentence_transformers import SentenceTransformer
import os
import torch
import numpy as np
import time

print("CUDA available:", torch.cuda.is_available())
print("CUDA version:", torch.version.cuda)

class EmbedHost:
    def __init__(self, local=True, model_name="multilingual-e5-large-instruct", model_path=r"E:\Coding\LLM", retry_times=3, wait_time=1):
        if local:
            current_directory = os.getcwd()
            os.chdir(model_path)  # 先迁移到大模型文件夹
            self.model = SentenceTransformer(model_name_or_path=model_name, local_files_only=True)
            os.chdir(current_directory)  # 换回原来的目录下
        else:
            self.model = SentenceTransformer(model_name_or_path=model_name)
        
        self.retry_times = retry_times  # 设置最大重试次数
        self.wait_time = wait_time  # 设置每次重试前等待的时间（秒）

    def encode(self, sentence):
        """带有重试机制的句子嵌入"""
        for attempt in range(self.retry_times):
            try:
                return self.model.encode(sentence)
            except RuntimeError as e:
                print(f"Runtime error during encoding attempt {attempt + 1}. Error: {e}")
                if attempt < self.retry_times - 1:  # 如果不是最后一次尝试
                    time.sleep(self.wait_time)  # 等待一段时间后重试
                else:
                    print(f"All {self.retry_times} attempts failed. Returning an empty numpy array.")
                    return np.array([])  # 所有尝试失败后返回空数组
    
    def compare2sentence(self, sentence1, sentence2):
        "比较两个句子的相似度，返回一个数"
        embedding1 = self.encode(sentence1)
        embedding2 = self.encode(sentence2)
        
        if embedding1.size == 0 or embedding2.size == 0:
            print("由于之前的编码错误，无法计算相似度。")
            return np.nan
        
        # 注意：SentenceTransformer 的 similarity 方法可能不直接接受编码结果作为参数。
        # 下面的代码假设存在这样一个方法，如果不存在，请使用适当的方法计算相似度。
        similarity = self.model.similarity([embedding1], [embedding2])
        return similarity[0][0]