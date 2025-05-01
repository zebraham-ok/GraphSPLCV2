import requests
import json
from urllib.parse import quote  # 用于 URL 编码
import os
from text_process.file_process import sanitize_filename
import time
from API.Mongo_SPLC import MongoDBManager
from random import random
from .secret_manager import read_secrets_from_csv

current_file = os.path.abspath(__file__)
current_dir = os.path.dirname(os.path.dirname(current_file)) # 回到项目文件夹下
secret_file=os.path.join(current_dir,"API","secrets.csv")
secret_dict=read_secrets_from_csv(filename=secret_file)

bearer_token = secret_dict["liang_google"]
zone="serp_api1"

class LiangGoogleAPI4Company:
    def __init__(self, bearer_token=bearer_token, zone=zone, freshness_limit=30, default_collection="google_search_results"):
        self.mongo = MongoDBManager()  # 初始化MongoDB客户端
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {bearer_token}"
        }
        self.liang_url = "https://api.brightdata.com/request"
        self.zone = zone
        self.default_collection = default_collection  # 默认集合名称
        self.freshness_limit=freshness_limit
        
    def liang_google_search(self, search_query, num=96, since=None, until=None):
        '进行简单搜索（不含MongoDB交互）'
        # 对搜索查询进行 URL 编码
        query_with_time=self._build_time_constrained_query(search_query, since, until)
        # print(query_with_time)
        encoded_search_query = quote(query_with_time)
        
        data = {
            "zone": self.zone,
            # "url": f"https://www.google.com/search?q={encoded_search_query}&brd_json=1",
            "url": f"https://www.google.com/search?q={encoded_search_query}&brd_json=1&gl=sg&num={num}&location=Singapore&uule=w+CAIQICIJU2luZ2Fwb3Jl",
            "format": "json"
        }
        
        try:
            response = requests.post(self.liang_url, headers=self.headers, data=json.dumps(data))
            response.raise_for_status()  # Raise an exception for HTTP errors
            response_text=response.text
            if response_text:
                result_dict=json.loads(response_text)
                if "body" in result_dict:
                    # print(type(result_dict["body"]))
                    return json.loads(result_dict["body"])
            print("Returned don't contain body")
            return {}
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
            return {}
        except json.JSONDecodeError as e:
            print(f"cannot decode {result_dict}")
            return {}
        
    def _build_time_constrained_query(self, base_query, since=0, until=0):
        """构建时间限定查询语句"""
        query = base_query
        if since and 1950 < since < 2050:
            query += f" after:{since}/01/01"
        if until and 1950 < until < 2050:
            query += f" before:{until}/12/31"
        return query
    
    def convert_to_bing_format(self, google_data, query, since, until):
        """将完整的Google数据转换为Bing格式"""
        organic_results = google_data.get("organic", [])
        general_info = google_data.get("general", {})
        input_info = google_data.get("input", {})

        bing_results = []
        for result in organic_results:
            bing_entry = {
                "id": result.get("link", ""),
                "name": result.get("title", ""),
                "url": result.get("link", ""),
                "snippet": result.get("description", ""),
                # 添加日期扩展字段
                "datePublished": result.get("extensions", [{}])[0].get("text", "") if result.get("extensions") else ""
            }
            bing_results.append(bing_entry)

        return {
            "_type": "FROM LIANG GOOGLE",
            "queryContext": {
                "originalQuery": query,
                "since": since,
                "until": until
            },
            "timestamp": time.time(),
            "expire_at": time.time() + self.freshness_limit*86400,
            "webPages": {
                "webSearchUrl": input_info.get("original_url", "https://www.google.com/search"),
                "totalEstimatedMatches": general_info.get("results_cnt", len(organic_results)),
                "value": bing_results
            },
            "relatedSearches": {
                "value": self._extract_related_searches(google_data.get("navigation", []))
            }
        }
        
    def _extract_related_searches(self, navigation):
        """提取相关搜索建议"""
        return [{"text": item["title"], "url": item["href"]} for item in navigation if "href" in item]
    
    def _generate_filename(self, search_term):
        """生成安全文件名"""
        return f"{sanitize_filename(search_term)}-google.json"
    
    def _get_cache_query_bing(self, query_params):
        """构建缓存查询条件"""
        return {
            "queryContext.originalQuery": query_params.get("query"),
            "queryContext.since": query_params.get("since"),
            "queryContext.until": query_params.get("until")
        }
        
    def _get_cache_query_google(self, query_params):
        """构建缓存查询条件（适用于谷歌检索）"""
        return {
            "general.query": query_params.get("query")
        }

    def _get_cached_results(self, query_params, bing=True):
        """从MongoDB获取缓存结果"""
        if bing:
            cache_query = self._get_cache_query_bing(query_params)
        else:
            cache_query = self._get_cache_query_google(query_params)
            
        # print(cache_query)
        # projection = {"results": 1, "timestamp": 1}
        
        try:
            # 查找最新缓存记录
            # print(self.default_collection)
            latest_cache = self.mongo.find(
                filter_query=cache_query,
                collection=self.default_collection,
                # projection=projection,
                limit=1,
                sort_list_dict=[{"timestamp", -1}]
            )
            
            # if latest_cache and self._is_cache_valid(latest_cache[0]):
            #     return latest_cache[0]
            if latest_cache:
                return latest_cache[0]
            return None
        except Exception as e:
            print(f"缓存查询失败: {str(e)}")
            return None

    # def _is_cache_valid(self, cache_record):
    #     """检查缓存有效性"""
    #     cache_time = cache_record.get("timestamp", 0)
    #     return (time.time() - cache_time) <= self.freshness_limit * 86400

    def _save_to_mongodb(self, results):
        """保存结果到MongoDB"""
        try:
            return self.mongo.save(results, self.default_collection)
        except Exception as e:
            print(f"保存到MongoDB失败: {str(e)}")
            return None

    def execute_search_with_mongo(self, query, num=96, since=None, until=None, convert_bing=True):
        """借助MongoDB作为缓存工具进行亮数据谷歌搜索"""
        # 构建查询参数
        query_params = {
            "query": query,
            "since": since,  # 可根据实际情况添加
            "until": until
        }
        
        # 尝试获取缓存
        cached = self._get_cached_results(query_params, bing=convert_bing)
        if cached:
            # print("Using Cached Result")
            return cached
        
        # print("using new")
        google_data = self.liang_google_search(search_query=query, num=num, since=since, until=until)
        if convert_bing:
            google_data=self.convert_to_bing_format(google_data, query, since, until)
            self._save_to_mongodb(google_data)
        else:
            self._save_to_mongodb(google_data)
        
        return google_data

if __name__=="__main__":
    search_query = "芯源微+供应商"  # 包含中文和空格的搜索词
    google_host=LiangGoogleAPI4Company()
    results = google_host.liang_google_search(search_query, num=95, since=2010, until=2015)
    print(results)