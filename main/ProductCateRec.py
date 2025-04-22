import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from text_process.find_json import get_dict_from_str
from tqdm import tqdm
# import API.ai_ask
import API.neo4j_SPLC
from API.ai_ask import get_qwen_embedding, ask_qwen
import time

EMPTY_SLEEP=600

class ProductClassifier:
    def __init__(self, max_workers=15, neo4j_host=API.neo4j_SPLC.Neo4jClient(driver=API.neo4j_SPLC.local_driver)):
        self.neo4j = neo4j_host
        self.batch_size = 100
        self.max_workers = max_workers  # 根据实际CPU核心数调整

    def fetch_unclassified_products(self):
        """获取未分类的产品记录"""
        return self.neo4j.execute_query('''
            MATCH ()-[r:SupplyProductTo]->()
            WHERE r.product IS NOT NULL and r.product <> "" AND r.product_category IS NULL
            WITH r.product AS p_name, elementid(r) AS r_id, r.analysing_process AS content
            MERGE (p:Product {name: p_name})
            RETURN elementid(p) AS p_id, p_name, r_id, content
            order by rand()
            LIMIT $batch_size
        ''', parameters={"batch_size": self.batch_size})

    def process_single_product(self, record):
        """处理单个产品分类任务"""
        p_id = record["p_id"]
        p_name = record["p_name"]
        r_id = record["r_id"]
        content = record["content"]

        # 检查是否存在已有分类（这个虽然会变快很多，但是可能会导致没有结合原本进行判断，忽略了公司和文本的具体情境”
        # if self.check_existing_category(p_name, r_id):
        #     return {"status": "skipped", "reason": "existing category"}

        # 向量搜索相似分类
        search_results = self.vector_search(p_name)
        if not search_results:
            return {"status": "failed", "reason": "no search results"}

        # AI分类判断
        classification = self.ai_classify(p_name, search_results, content)
        if not classification:
            return {"status": "failed", "reason": "ai classification failed"}

        # 更新数据库
        return self.update_database(p_id, r_id, classification, search_results)

    def check_existing_category(self, p_name, r_id):
        """检查是否已有分类"""
        existing = self.neo4j.execute_query('''
            MATCH (p:Product)-[r:BelongToCategory]->(pc:ProductCategory)
            WHERE p.name = $name
            WITH pc
            MATCH ()-[r:SupplyProductTo]->()
            WHERE elementid(r) = $r_id
            SET r.product_category = pc.full_name
            RETURN pc.full_name AS full_name
        ''', parameters={"name": p_name, "r_id": r_id})
        
        if existing:
            print(f"{p_name} 已分类至 {existing[0]['full_name']}")
            return True
        return False

    def vector_search(self, p_name):
        """执行向量搜索"""
        query_vector = get_qwen_embedding(p_name)
        return self.neo4j.execute_query('''
            CALL db.index.vector.queryNodes('productcategory_qwen_embedding_index', 15, $query_vector)
            YIELD node, score
            WHERE (node.rubbish <> true OR node.rubbish IS NULL)
            RETURN 
                elementid(node) AS id,
                node.full_name AS cate_full_name,
                node.name AS cate_name,
                node.category_code AS code
            ORDER BY score DESC
        ''', parameters={"query_vector": query_vector})

    def ai_classify(self, p_name, search_results, content):
        """调用AI进行分类判断"""
        selection_dict = {i["cate_full_name"]: i["code"] for i in search_results}
        
        prompt = self.build_prompt(p_name, selection_dict, content)
        response = ask_qwen(
            prompt_text=prompt,
            system_instruction="你是产品分类专家，熟读分类目录，用JSON格式回答",
            temperature=0.1,
            mode="json",
            # model="qwen-turbo"
            model="deepseek-v3"
            # model="deepseek-r1"
        )
        
        try:
            list_of_dict=get_dict_from_str(response)
            if list_of_dict:
                return list_of_dict[0]
            else:
                print(f"AI响应解析失败：{response}")
        except json.JSONDecodeError:
            print(f"AI响应解析失败：{response}")
            return None

    def build_prompt(self, p_name, selection_dict, content):
        """构建提示模板"""
        base_prompt = f'''
        你的选择范围如下：
            selection_range={selection_dict}
        1. 首先，请重复产品的名称，写在product一栏，作为结果的第一项
        2. 请基于原文给出你的分析过程，写在analysis一栏，作为结果的第二项
        3. 请从selection_range中选出你认为最符合该产品的类别，将其完整简体中文名称写在product_category_full_name一栏，作为结果的第三项
        4. 最后，请写出selection_range中该名称对应的编码，用字符串格式，写在category_code一栏，作为结果的第四项
        {{
            "product": str
            "analysis": str,
            "product_category_full_name": str, 
            "category_code": str
        }}
        请务必准确、规范，使用json格式进行回答。字典中的键使用英语，值尽量使用汉语。
    '''
        
        if content:
            return f"请看一段文本：{content}，分析文本中的“{p_name}”与下列哪一种产品类别最接近。\n"+base_prompt
        return f"请分析产品“{p_name}”与下列哪一种产品类别最接近。\n"+base_prompt

    def update_database(self, p_id, r_id, classification, search_results):
        """更新数据库记录"""
        code = str(classification.get("category_code"))
        category_map = {i["code"]: (i["id"], i["cate_full_name"]) for i in search_results}
        
        if code not in category_map:
            print(f"无效的分类编码: {code}，allowed names are: {category_map}")
            return {"status": "failed", "reason": "invalid code"}
        
        node_id, full_name = category_map[code]
        expected_names = (full_name, search_results[0]["cate_name"])
        
        if classification["product_category_full_name"] not in expected_names:
            print(f"名称不匹配: {classification['product_category_full_name']}，allowed names are: {category_map}")
            return {"status": "failed", "reason": "name mismatch"}
        
        # 创建分类关系
        self.neo4j.Crt_rel_by_id(p_id, node_id, "BelongToCategory")
        # 更新供应关系
        self.neo4j.execute_query('''
            MATCH ()-[r:SupplyProductTo]->() 
            WHERE elementid(r) = $r_id 
            SET r.product_category = $category
        ''', parameters={"r_id": r_id, "category": full_name})
        
        return {"status": "success", "category": full_name}

    def run(self):
        """主执行流程"""
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            while True:
                records = self.fetch_unclassified_products()
                if not records:
                    print(f"没有未处理的章节，休眠 {EMPTY_SLEEP} 秒...")
                    time.sleep(EMPTY_SLEEP)
                    continue
                
                futures = {
                    executor.submit(self.process_single_product, record): record 
                    for record in records
                }
                
                with tqdm(total=len(futures), desc="处理产品分类") as pbar:
                    for future in as_completed(futures):
                        try:
                            result = future.result()
                            # 在此添加错误处理逻辑
                        except Exception as e:
                            print(f"处理失败: {str(e)}")
                        finally:
                            pbar.update(1)

def product_cate_rec_main(neo4j_host=None, max_workers=15):
    "进行产品识别的主函数（这个一般在关系验证之后）"
    if neo4j_host:
        classifier = ProductClassifier(max_workers, neo4j_host)
    else:
        classifier = ProductClassifier(max_workers)
    classifier.run()