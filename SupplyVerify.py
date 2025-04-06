
import API.ai_ask
import API.neo4j_SLPC
from text_process.find_json import get_dict_from_str
neo4j_host=API.neo4j_SLPC.Neo4jClient()
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
import time

def ai_splc_verify(supplier, customer, s_des, c_des, content, s_produce_list, c_produce_list, s_need_list, c_need_list):
    "进行供应链关系和方向的判断"
    # 这一步需要的中国特有知识较多，因此使用qwen更加准确
    ai_response=API.ai_ask.ask_gpt(prompt_text=f'''
            请查看如下两个实体的信息，以及一个文本片段，请依照原文文本内容，按照如下步骤，推测两个实体之间的供应链关系：
            {{
                "entity1": "{supplier}", "description1": "{s_des}", 
                "can_produce1": "{s_produce_list}", "need1": "{s_need_list}",
                "entity2": "{customer}", "description2": "{c_des}", 
                "can_produce2": "{c_produce_list}", "need2": "{c_need_list}",
                "section_content": {content}
            }}
            1. 供应商是提供产品或服务并收取费用的一方。客户是接受产品或服务并付费的一方。{content}中的信息是否能够明确说明{supplier}和{customer}之间存在“供应商-客户”关系？请先在analysing_process出简要写出你的推断过程
            2. 原文是否有充足证据表明两个实体存在“供应商-客户”关系？请在exist_relationship位置，用true或false写出你的判断。
            3. 如果存在，你认为两个实体当中，哪个是供应商？哪个是客户？请两个实体的名称写在supplier和customer的对应的位置。
            4. 请判断该供应关系是暂时性的（Temporary）如一次性的中标；持续性的（Continuous）供货；未来可能的（Future）；还是已经断供或终止（Stopped），在这三个单词当中选择一个填写在status位置。
            5. 请根据两个entity生产和所需的产品类型，推断在他们的供货关系中，传递的产品或服务是什么，用一个尽可能具体的词填写在product位置。
            6. 请查看section_content中是否有对于这个供应链关系规模的说明，比如货物的数量（amount）有几个、几吨（amount_unit)等等，以及货物的价值（value）有多少人民币或美元（value_unit）等等，并填写在对应的位置。如果文中没有提及，请在对应位置填写空字符串""。
            请以json的格式汇总上述判断内容，按照如下格式进行填写
            {{
                "analysing_process": str,
                "exist_relationship": bool,
                "supplier": str,
                "customer": str,
                "status": str,
                "product": str,
                "amount": str,
                "amount_unit": str,
                "value": str,
                "value_unit": str
            }}
            注意，回答时请严格基于事实，尽可能使用简体中文，按照规定格式
        ''',
            system_instruction="你是一个供应链信息采集员，用json的标准化格式回答用户的提问",
            # mode="json",
            model="gpt-4o",
            temperature=0.05,
            # enable_search=True
        )
    return get_dict_from_str(ai_response)


class Neo4jHandler:
    def __init__(self):
        self.client = API.neo4j_SLPC.Neo4jClient()
        
    def fetch_records(self, batch_size=50):
        """获取未验证的供应链关系记录"""
        query = '''
        MATCH (n)-[r:SupplyProductTo]->(m), (s:Section)
        WHERE r.verified IS NULL AND elementid(s)=r.reference_section
        RETURN 
            n.name AS supplier, elementid(n) AS s_id, n.description AS s_des,
            m.name AS customer, elementid(m) AS c_id, m.description AS c_des,
            s.content AS content, elementid(r) AS r_id
        LIMIT $limit'''
        return self.client.execute_query(query, parameters={'limit': batch_size})
    
    def get_products(self, entity_id, rel_type):
        """获取实体生产/需求的产品列表"""
        query = f'MATCH (n)-[:{rel_type}]->(p) WHERE elementid(n)=$id AND n.rubbish<>true RETURN p.name AS name limit 10'
        result = self.client.execute_query(query, parameters={'id': entity_id})
        return [item['name'] for item in result]
    
    def update_relationship(self, r_id, attributes):
        """更新关系属性"""
        query = '''
        MATCH ()-[r]->()
        WHERE elementid(r)=$r_id
        SET r.verified=$verified, r.analysing_process=$process, 
            r.status=$status, r.product=$product, r.amount=$amount, 
            r.amount_unit=$amount_unit, r.value=$value, r.value_unit=$value_unit'''
        self.client.execute_query(query, parameters={
            'r_id': r_id, **attributes
        })
    
    def create_reverse_relationship(self, start_id, end_id, attributes):
        """创建反向供应链关系"""
        query = '''
        MATCH (a),(b) 
        WHERE elementid(a)=$start AND elementid(b)=$end
        MERGE (a)-[r:SupplyProductTo]->(b)
        SET r += $attrs'''
        self.client.execute_query(query, parameters={
            'start': start_id, 'end': end_id, 'attrs': attributes
        })
    
    def mark_as_suspected(self, r_id):
        """标记关系为可疑"""
        self.client.execute_query(
            "MATCH ()-[r]->() WHERE elementid(r)=$id SET r.verified='suspected'",
            parameters={'id': r_id}
        )
        
    def mark_as_name_not_align(self, r_id):
        """标记关系为可疑"""
        self.client.execute_query(
            "MATCH ()-[r]->() WHERE elementid(r)=$id SET r.verified='false_name'",
            parameters={'id': r_id}
        )

def process_record(record, neo_handler: Neo4jHandler):
    """处理单条记录的多线程任务"""
    try:
        # 提取记录信息
        s_id, c_id = record['s_id'], record['c_id']
        s_info = {
            'produce': neo_handler.get_products(s_id, 'Produce'),
            'need': neo_handler.get_products(s_id, 'Need')
        }
        c_info = {
            'produce': neo_handler.get_products(c_id, 'Produce'),
            'need': neo_handler.get_products(c_id, 'Need')
        }

        # AI验证
        ai_result = ai_splc_verify(
            record['supplier'], record['customer'],
            record['s_des'], record['c_des'], record['content'],
            s_info['produce'], c_info['produce'], s_info['need'], c_info['need']
        )[0]

        # 处理验证结果
        if not ai_result.get('exist_relationship', False):
            neo_handler.mark_as_suspected(record['r_id'])
            return

        # 验证名称一致性（名称不一致可能只是大模型犯傻了，因此可以事后一起重新运行一次，但如果不标记出来可能会导致阻塞）
        valid_direction = {ai_result['supplier'], ai_result['customer']} == {record['supplier'], record['customer']}
        if not valid_direction:
            print(f"名称不一致: {ai_result['supplier']}，{ai_result['customer']} vs {record['supplier']}，{record['customer']}")
            neo_handler.mark_as_name_not_align(record['r_id'])
            return

        # 构建属性字典
        attrs = {
            'verified': True,
            'process': ai_result.get('analysing_process', ''),
            'status': ai_result.get('status', ''),
            'product': ai_result.get('product', ''),
            'amount': ai_result.get('amount', ''),
            'amount_unit': ai_result.get('amount_unit', ''),
            'value': ai_result.get('value', ''),
            'value_unit': ai_result.get('value_unit', '')
        }

        # 更新数据库
        if ai_result['customer'] == record['customer']:
            neo_handler.update_relationship(record['r_id'], attrs)
        else:
            neo_handler.mark_as_suspected(record['r_id'])
            neo_handler.create_reverse_relationship(c_id, s_id, attrs)
            
    except Exception as e:
        print(f"处理记录{record.get('r_id','')}时出错: {str(e)}")

def main():
    neo_handler = Neo4jHandler()
    with ThreadPoolExecutor(max_workers=20) as executor:
        while True:
            records = neo_handler.fetch_records()
            if not records:
                print("当前内容全部处理完成，休眠10分钟")
                time.sleep(600)
            
            # 创建进度条
            with tqdm(total=len(records), desc="处理进度", unit="条") as pbar:
                # 提交任务并添加完成回调
                futures = []
                for record in records:
                    future = executor.submit(process_record, record, neo_handler)
                    future.add_done_callback(lambda _: pbar.update())
                    futures.append(future)
                
                # 等待所有任务完成
                for future in futures:
                    future.result()

if __name__ == "__main__":
    main()