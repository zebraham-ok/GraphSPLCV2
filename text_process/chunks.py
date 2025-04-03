from langchain_text_splitters import RecursiveCharacterTextSplitter
from langdetect import detect_langs

# 自定义适合中日文的分隔符列表
separators = [
    "\n\n", "\n", "。", "！", "？", ". ", "!", "?"
    "\u3002",  # 中日文句号
]

dense_lang=["zh","ko","ja","ar","th"]

# 使用正则表达式的“或”操作符 "|" 合并所有分隔符，并按优先级排序
separate_reg = r"\n{1,}|。|！|？|\. |! |\?"

class text_splitter_zh_en:
    def __init__(self, zh_max_len:int, en_max_len:int, overlap_ratio):
        self.zh_spliter=RecursiveCharacterTextSplitter(separators=[separate_reg],  # 将所有分隔符合并为一个正则表达式模式
                chunk_size=zh_max_len,  # 每个块的目标大小
                chunk_overlap=int(zh_max_len*overlap_ratio),  # 块之间的重叠大小
                length_function=len,
                is_separator_regex=True,  # 使用正则表达式模式
            )

        self.en_spliter=RecursiveCharacterTextSplitter(separators=[separate_reg],  # 将所有分隔符合并为一个正则表达式模式
                chunk_size=en_max_len,  # 每个块的目标大小
                chunk_overlap=int(en_max_len*overlap_ratio),  # 块之间的重叠大小
                length_function=len,
                is_separator_regex=True,  # 使用正则表达式模式
            )
        
    def split_str(self, text:str):
        "根据文本的语言类型，决定使用哪个切分长度"
        
        # 如果属于亚洲语系，则切分成更短的文本
        try:
            lang=detect_langs(text)[0].lang[:2]
            if lang in dense_lang:
                spliter=self.zh_spliter
            else:
                spliter=self.en_spliter
        except Exception as e:
            spliter=self.en_spliter
            
        result_list=spliter.split_text(text)
        result_list=[i for i in result_list if (len(i)>1)]
        final_result_list=[]
        for sec in result_list:
            if sec[0] in separators:
                final_result_list.append(sec[1:].strip())
            elif sec[:2] in separators:
                final_result_list.append(sec[2:].strip())
            else:
                final_result_list.append(sec.strip())
        return final_result_list

# def get_language(s):
#     "看应该用什么语言的句号进行分割"
#     cn_count=s.count("。")
#     s=s.replace(".\n",". ")
#     en_count=s.count(". ")
#     if cn_count>en_count:
#         return "zh"
#     else:
#         return "en"

if __name__=="__main__":
    text_splitter = RecursiveCharacterTextSplitter(
        separators=[separate_reg],  # 将所有分隔符合并为一个正则表达式模式
        chunk_size=100,  # 每个块的目标大小
        chunk_overlap=50,  # 块之间的重叠大小
        length_function=len,
        is_separator_regex=True,  # 使用正则表达式模式
    )
    content='''Weaviate 是一种开源的类型向量搜索引擎数据库。
    Weaviate 允许您以类属性的方式存储 JSON 文档，同时将机器学习向量附加到这些文档上，以在向量空间中表示它们。
    Weaviate 可以独立使用（即带上您的向量），也可以与各种模块一起使用，这些模块可以为您进行向量化并扩展核心功能。
    Weaviate 具有 GraphQL-API，以便轻松访问您的数据。
    Weaviate 详细介绍：
    Weaviate 是一种低延迟的向量搜索引擎，支持不同的媒体类型（文本、图像等）。它提供语义搜索、问答提取、分类、可定制模型（PyTorch/TensorFlow/Keras）等功能。Weaviate 从头开始使用 Go 构建，可以存储对象和向量，允许将向量搜索与结构化过滤器和云原生数据库的容错性结合起来。通过 GraphQL、REST 和各种客户端编程语言都可以访问它。
    官网地址：https://weaviate.io/
    ————————————————

                                版权声明：本文为博主原创文章，遵循 CC 4.0 BY-SA 版权协议，转载请附上原文出处链接和本声明。
                            
    原文链接：https://blog.csdn.net/Uluoyu/article/details/140798169'''
    text_splitter_zh_en_host=text_splitter_zh_en(zh_max_len=100, en_max_len=200, overlap_ratio=0.2)
    text_splitter_zh_en_host.split_str(spliter=text_splitter,text=content,separators=separators)