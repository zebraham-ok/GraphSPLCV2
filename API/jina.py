import requests

class JinaClient():
    def __init__(self,jina_key="jina_3402fff132dd486d8b7f1d775b15f1edHyVaeETMue99CZ6cSoM5CMsJrK-z",proxy=""):
        self.jina_key=jina_key
        self.proxy=proxy


    def url_reader(self,url,use_key=False,timeout=10,use_proxy=False):
        "使用jina读取网页"
        jina_url = f"https://r.jina.ai/{url}"
        headers = {
            "Accept": "application/json",
            "X-Base": "final",
            "X-Timeout": str(timeout),
            "X-With-Images-Summary": "true",
            "X-With-Links-Summary": "true"
        }
        
        if use_key:
            headers["Authorization"]=f"Bearer {self.jina_key}"
            
        if use_proxy:
            headers["X-Proxy-Url"]=self.proxy
        
        try:
            response = requests.get(jina_url, headers=headers)
            response.raise_for_status()  # 如果响应状态码不是200，则抛出HTTPError
            return response.json()  # 返回json格式的响应数据
        except requests.exceptions.RequestException as err:
            print(f"请求出现错误: {err}")
            return None

    def segment_text(self, content, use_key=False, 
                    return_tokens=False, return_chunks=True, max_chunk_length=1000):
        """
        发送文本分段请求到 Jina AI API。

        :param content: 要分段的文本内容
        :param token: 授权令牌，默认值为示例中的令牌
        :param return_tokens: 是否返回标记，默认为 False
        :param return_chunks: 是否返回分段，默认为 True
        :param max_chunk_length: 每个分段的最大长度，默认为 1000
        :return: API 响应的 JSON 数据
        """
        url = 'https://api.jina.ai/v1/segment'
        headers = {
            "Content-Type": "application/json",
        }
        
        if use_key:
            headers["Authorization"]=f"Bearer {self.jina_key}"
        
        payload = {
            "content": content,
            "return_tokens": return_tokens,
            "return_chunks": return_chunks,
            "max_chunk_length": max_chunk_length
        }

        response = requests.post(url, json=payload, headers=headers)

        # 检查响应状态码
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Request failed with status code {response.status_code}: {response.text}")

# 示例调用
if __name__=="__main__":
    jina_client=JinaClient()
    read_result=jina_client.url_reader(url="https://baike.baidu.com/item/%E5%96%9C%E5%AE%B6%E5%BE%B7/7677583")
    read_result["data"]