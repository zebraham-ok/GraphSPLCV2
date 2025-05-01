from .secret_manager import read_secrets_from_csv
# import os
import logging
from typing import Dict, List, Union
from pymongo import MongoClient, errors
from pymongo.database import Database
from pymongo.collection import Collection
from retry import retry
from dotenv import load_dotenv
from contextlib import contextmanager

# 从密码文件中读取
secret_file=r"API\secrets.csv"
secret_dict=read_secrets_from_csv(filename=secret_file)    

# 加载环境变量（根据实际情况调整）
load_dotenv()

class MongoDBManager:
    """智能MongoDB管理客户端，支持自动连接和重试机制"""
    
    _instance = None  # 单例实例
    
    def __new__(cls, *args, **kwargs):
        """单例模式实现"""
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if not hasattr(self, '_initialized'):
            self._initialized = True
            self._client = None
            self._db = None
            
            # 配置参数（从secret.csv中获取）
            self.MONGO_URI = secret_dict.get("mongo_url", "mongodb://localhost:27017/")
            self.DB_NAME = secret_dict.get("mongo_db", "my_database")
            self.default_collection = "webpage"
            self.connection_timeout = 5  # 秒
            self.max_pool_size = 100
            
            # 配置日志
            logging.basicConfig(level=logging.INFO)
            self.logger = logging.getLogger("MongoDBManager")
            
            # 初始化连接
            self._ensure_connection()

    def _ensure_connection(self):
        """确保有效连接"""
        try:
            if not self._client or not self._client.is_primary:
                self._client = MongoClient(
                    self.MONGO_URI,
                    serverSelectionTimeoutMS=self.connection_timeout*1000,
                    connectTimeoutMS=10*1000,
                    maxPoolSize=self.max_pool_size
                )
                self._db = self._client[self.DB_NAME]
                self._client.admin.command('ping')
                self.logger.info("Successfully connected to MongoDB")
        except errors.PyMongoError as e:
            self.logger.error(f"Connection error: {str(e)}")
            raise

    @contextmanager
    def get_collection(self, collection_name: str = None) -> Collection:
        """获取集合的上下文管理器"""
        self._ensure_connection()
        try:
            yield self._db[collection_name or self.default_collection]
        except errors.PyMongoError as e:
            self.logger.error(f"Operation failed: {str(e)}")
            raise
        finally:
            # 保持长连接，不主动关闭
            pass
    
    @retry(tries=3, delay=1, backoff=2, exceptions=errors.PyMongoError)
    def list_collection_names(self) -> List[str]:
        """获取当前数据库中所有集合的名称列表（自动重试）"""
        self._ensure_connection()
        try:
            return self._db.list_collection_names()
        except errors.PyMongoError as e:
            self.logger.error(f"获取集合列表失败: {str(e)}")
            raise

    @retry(tries=3, delay=1, backoff=2, exceptions=errors.PyMongoError)
    def insert_one(self, data: Dict, collection: str = None) -> str:
        """智能插入单文档（自动重试）"""
        with self.get_collection(collection) as col:
            result = col.insert_one(data)
            self.logger.debug(f"Inserted ID: {result.inserted_id}")
            return str(result.inserted_id)

    @retry(tries=3, delay=1, backoff=2, exceptions=errors.PyMongoError)
    def insert_many(self, data_list: List[Dict], collection: str = None, 
                   batch_size: int = 100) -> List[str]:
        """批量插入文档（自动分批）"""
        with self.get_collection(collection) as col:
            inserted_ids = []
            for i in range(0, len(data_list), batch_size):
                batch = data_list[i:i + batch_size]
                result = col.insert_many(batch)
                batch_ids = [str(id_) for id_ in result.inserted_ids]
                inserted_ids.extend(batch_ids)
                self.logger.debug(f"Inserted batch {i//batch_size}: {len(batch_ids)} docs")
            return inserted_ids

    @retry(tries=3, delay=1, backoff=2, exceptions=errors.PyMongoError)
    def find(self, filter_query: Dict, collection: str = None, 
            projection: Dict = None, limit: int = 0, sort_list_dict: List = []) -> List[Dict]:
        """通用查询方法"""
        with self.get_collection(collection) as col:
            if sort_list_dict:
                cursor = col.find(filter_query, projection).sort(sort_list_dict).limit(limit)
            else:
                cursor = col.find(filter_query, projection).limit(limit)
            return list(cursor)

    @staticmethod
    def save(data: Union[Dict, List[Dict]], collection: str = None, 
            batch_size: int = 100) -> Union[str, List[str]]:
        """统一保存入口（自动类型判断）"""
        instance = MongoDBManager()
        if isinstance(data, list):
            return instance.insert_many(data, collection, batch_size)
        return instance.insert_one(data, collection)

# 使用示例
if __name__ == "__main__":
    # 完全无需手动管理连接
    # 单条插入
    doc_id = MongoDBManager.save({"name": "test", "value": 123})
    print(f"Inserted ID: {doc_id}")
    
    print(f"包含的所有collection：{MongoDBManager.list_collection_names()}")
    
    # 批量插入
    docs = [{"item": f"product_{i}"} for i in range(500)]
    ids = MongoDBManager.save(docs, "products")
    print(f"Inserted {len(ids)} documents")
    
    # 直接查询
    results = MongoDBManager().find({"value": {"$gt": 100}}, limit=5)
    print(f"Found {len(results)} matching documents")