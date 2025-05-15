"将neo4j中的数据导出到nx格式"
import networkx as nx
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from API import neo4j_SPLC, SQL_SPLC
from collections import Counter
from numpy import nan

LABEL_PRIORITY = [
    "Company", "Government", "Media", "MiningSite", "Academic", 
    "NGO", "Factory", "Others", "EntityObj"
]

# 如果一个节点已经存在于MySQL的company_main当中了，NODE_ATTR中的属性会优先听从company_main中的投票结果，仅当company_main中是空值的时候，才会看NODE_ATTR里面的情况
NODE_ATTR = ["country", "industry_1st", "industry_2nd", "category_1st", "category_2nd", "category_3rd"]

EDGE_ATTR = ["product", "product_category", "status"]

FORBID_VALUE=["N/A","nan","NaN","Nan",nan]

sql_host=SQL_SPLC.generate_sql_host(database="splc")

def is_rubbish(name):
    "判断一个名称是否是垃圾节点"
    return sql_host.check_item_exists(table_name="rubbish_nodes", item=name, strict_equal=False)

def most_frequent(lst, forbid_value=FORBID_VALUE):
    "返回一个列表中出现频次最多的元素"
    if not lst:
        return ""
    count = Counter(lst)
    max_count = max(count.values())
    most_common_element = next((key for key, value in count.items() if value == max_count and key!=forbid_value), "")
    return most_common_element

def get_needed_value(dic, needed_attrs, fill=None):
    "保留指定类型的属性，并将缺失值进行填充。如果fill不填则忽略该变量"
    result_dict={}
    for key in needed_attrs:
        if key in dic:
            value=dic[key]
            if value and (value not in FORBID_VALUE):  # 如果有这个变量，但是是空的，那么也要
                result_dict[key]=dic[key]
            elif fill:
                result_dict[key]=fill
        elif fill:  # 如果不给fill的值，略过
            result_dict[key]=fill
    return result_dict

class GraphBuilder:
    def __init__(self):
        self.G = nx.DiGraph()
        self.relation_counter = {}
        self.lock = Lock()
        self.pbar = None

    def match_label(self, labels):
        """带优先级的标签匹配"""
        for label in LABEL_PRIORITY:
            if label in labels:
                return label
        return ""

class Neo4jExporter:
    def __init__(self, neo4j_host:neo4j_SPLC.Neo4jClient, sql_host: SQL_SPLC.MySQLClient = sql_host, node_attr_list=NODE_ATTR, rel_attr_list=EDGE_ATTR):
        self.neo = neo4j_host
        self.sql = sql_host
        self.batch_size = 500
        self.driver_lock = Lock()
        self.node_attr_list=node_attr_list
        self.rel_attr_list=rel_attr_list

    def fetch_relations(self, skip, start_year=None, end_year=None, year=None):
        """线程安全的批次查询，融合了限定年份的查询"""
        with self.driver_lock:
            # 构造语句让特定的attr被返回
            s_attr_query=""
            c_attr_query=""
            r_collect_query=""
            r_attr_query=""
            for attr in self.node_attr_list:
                s_attr_query+=f"COALESCE(n.{attr}, '') as s_{attr},\n"
                c_attr_query+=f"COALESCE(m.{attr}, '') as c_{attr},\n"
            for attr in self.rel_attr_list:
                r_collect_query+=f", COLLECT(r.{attr}) as r_{attr}"
                r_attr_query+=f", r_{attr}"
            
            if year:
                year_argument=f" and r.time contains '{year}'"
            elif start_year and end_year:
                year_argument=f" and r.time>'{start_year}' and r.time<'{end_year}' "
            else:
                year_argument=""
            
            return self.neo.execute_query(f"""
                MATCH (n:EntityObj)-[r:SupplyProductTo]->(m:EntityObj)
                where n.rubbish <> true and m.rubbish <> true and r.verified <> "suspected"{year_argument}
                WITH n, m, COUNT(r) AS rel_count {r_collect_query}
                RETURN 
                    n.name AS supplier,
                    {s_attr_query}
                    labels(n) as s_labels,
                    elementid(n) as s_id,
                    m.name AS customer,
                    {c_attr_query}
                    labels(m) as c_labels,
                    elementid(m) as c_id,
                    rel_count{r_attr_query}
                ORDER BY supplier, customer
                SKIP {skip} LIMIT {self.batch_size}
            """)

    def process_batch(self, batch, builder, check_rubbish):
        """批量处理线程任务"""
        for record in batch:
            # 节点验证
            if check_rubbish:
                if is_rubbish(record["supplier"]):
                    self.neo.execute_query(f"MATCH (n) WHERE elementid(n)='{record['s_id']}' SET n.rubbish=true")
                    continue
                if is_rubbish(record["customer"]):
                    self.neo.execute_query(f"MATCH (n) WHERE elementid(n)='{record['c_id']}' SET n.rubbish=true")
                    continue

            # 线程安全更新
            with builder.lock:
                self.update_graph(builder, record)
                builder.pbar.update(1)

    def update_graph(self, builder, record):
        """原子化图更新操作"""
        
        def get_node_info(name):
            sql_query='''
                    SELECT * FROM company_synonym_semi
                    JOIN company_main on company_synonym_semi.entity_id=company_main.id
                    WHERE semi_name=:name
                '''
            sql_result=self.sql._execute_query(sql_query, params={"name": name}, dict_mode=True)
            if sql_result:
                sql_result_dict=sql_result[0]
                return sql_result_dict, sql_result_dict.get("entity_cn_name")
                # 如果要导出英文，直接修改这个为entity_en_name就可以了
            else:
                return {}, ""
        
        def update_node_info(node_info: dict, record, mode):
            "对record当中的节点信息进行更新，mode=s或c代表更新的是supplier还是customer"
            if not node_info:
                node_info={}
            else:
                node_info=get_needed_value(node_info, self.node_attr_list)
            for attr in self.node_attr_list:
                attr_value=node_info.get(attr)
                if attr_value and (attr_value not in FORBID_VALUE):
                    # 说明company_main已经提供了这个公司经过投票后的标准信息
                    continue
                else:
                    # 如果company_main表中没有提供信息，再从record当中添加信息
                    record_attr_value=record.get(f"{mode}_{attr}")
                    if record_attr_value and (record_attr_value not in FORBID_VALUE):
                        node_info[attr]=record_attr_value
            return node_info
        
        count = record["rel_count"]
        
        # 将record中的东西更新到两组node_info当中
        supplier_name=record["supplier"]
        supplier_info, supplier_standard_name=get_node_info(supplier_name)
        if supplier_standard_name:
            supplier=supplier_standard_name
        else:
            supplier=supplier_name
        filtered_supplier_info=update_node_info(node_info=supplier_info, record=record, mode="s")
        
        customer_name=record["customer"]
        customer_info, standard_customer_name=get_node_info(customer_name)
        if standard_customer_name:
            customer=standard_customer_name
        else:
            customer=customer_name
        filtered_customer_info=update_node_info(node_info=customer_info, record=record, mode="c")
        
        # 更新关系计数器
        rel_key = (supplier, customer)
        builder.relation_counter[rel_key] = builder.relation_counter.get(rel_key, 0) + count
        
        # 更新图结构
        if builder.G.has_edge(supplier, customer):
            builder.G[supplier][customer]['count'] += count
            for attr in self.rel_attr_list:
                builder.G[supplier][customer][attr] += record["r_"+attr]
            
        else:
            builder.G.add_edge(supplier, customer, count=count, 
                               **{attr: record["r_"+attr] for attr in self.rel_attr_list})
            builder.G.add_node(supplier, **filtered_supplier_info, 
                               type=builder.match_label(record["s_labels"]), name=str(supplier))
            builder.G.add_node(customer, **filtered_customer_info, 
                               type=builder.match_label(record["c_labels"]), name=str(customer))

    def export_supply_relations(self, max_workers=20, check_rubbish=False):
        """
            导出的主执行函数
            目前供应链关系还不是很多，还没有使用分批次导出的技巧
        """
        
        builder = GraphBuilder()
        # exporter = Neo4jExporter(neo4j_host)
        
        # 获取总记录数
        total = self.neo.execute_query("MATCH ()-[r:SupplyProductTo]->() where r.verified <> 'suspected' RETURN COUNT(r) AS total")[0]["total"]
        builder.pbar = tqdm(total=total, desc="Exporting relations")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            skip = 0
            futures = []
            while True:
                batch = self.fetch_relations(skip)
                if not batch:
                    break
                # 提交批次任务
                futures.append(executor.submit(self.process_batch, batch, builder, check_rubbish))
                skip += self.batch_size
            
            # 等待所有任务完成
            for future in futures:
                future.result()

        builder.pbar.close()
        return builder.G
    
    def export_supply_relations_by_year(self, year:int, max_workers=20, check_rubbish=False):
        """
            导出的主执行函数
            目前供应链关系还不是很多，还没有使用分批次导出的技巧
        """

        builder = GraphBuilder()
        # 获取总记录数
        total = self.neo.execute_query(f'''
            MATCH ()-[r:SupplyProductTo]->()
            where r.verified <> 'suspected' and r.time contains '{year}'
            RETURN COUNT(r) AS total''')[0]["total"]
        builder.pbar = tqdm(total=total, desc=f"Exporting {year} relations")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            skip = 0
            futures = []
            while True:
                batch = self.fetch_relations(skip, year=year)
                if not batch:
                    break
                # 提交批次任务
                futures.append(executor.submit(self.process_batch, batch, builder, check_rubbish))
                skip += self.batch_size
            
            # 等待所有任务完成
            for future in futures:
                future.result()

        builder.pbar.close()
        return builder.G.copy()
      
def adapt_to_export_form(G:nx.DiGraph, file_value=""):
    "将一个网络转化为适合导出的格式，主要是将列表形式的属性按照各个值出现的频次取其中频次最高的一个，同时保证缺失的属性用空字符串代替"
    for edge in G.edges:
        edge_info=G.edges[edge]
        for attr in EDGE_ATTR:
            if attr not in edge_info:
                edge_info[attr]=file_value
            elif isinstance(edge_info[attr], list):
                edge_info[attr]=most_frequent(edge_info[attr])
                
    for node in G.nodes:
        node_info=G.nodes[node]
        for attr in NODE_ATTR:
            if attr not in node_info:
                node_info[attr]=file_value
            elif isinstance(node_info[attr], list):
                node_info[attr]=most_frequent(node_info[attr])
    return G

def get_subnetwork(G, node_list, n_layers, tree_structure=False):
    """
    获取指定节点列表的上下游n层邻居构成的子网络，并保留原始属性。
    
    参数:
        G (nx.DiGraph): 输入的有向图。
        node_list (list): 指定的节点名称列表。
        n_layers (int): 上下游扩展的层数。
        tree_structure (bool): 是否通过复制节点保存双向树状结构。
                               如果为False，返回直接的子网络；
                               如果为True，返回树状结构的子网络。
    
    返回:
        nx.DiGraph: 包含目标节点及其上下游邻居的子网络（保留原始属性）。
    """
    if not isinstance(G, nx.DiGraph):
        raise ValueError("输入的图必须是有向图 (DiGraph)。")
    
    # 初始化结果图
    subgraph = nx.DiGraph()
    
    # 用于存储所有需要加入的节点和边
    nodes_to_add = set()
    edges_to_add = set()
    
    # 定义一个递归函数来获取上下游邻居
    def get_neighbors(node, current_layer, direction, origin):
        if current_layer > n_layers:
            return
        neighbors = []
        if direction == "upstream":
            neighbors = list(G.predecessors(node))
        elif direction == "downstream":
            neighbors = list(G.successors(node))
        
        for neighbor in neighbors:
            if tree_structure:
                # 为树状结构创建唯一的节点标识符
                unique_node = f"{neighbor}_L{current_layer}_{direction[0]}_{origin}"
                # 添加节点并复制属性
                subgraph.add_node(unique_node, **G.nodes[neighbor])
                # 添加边并复制属性
                edge_data = G.get_edge_data(neighbor, node) if direction == "upstream" else G.get_edge_data(node, neighbor)
                if edge_data:
                    subgraph.add_edge(unique_node, 
                                    node if current_layer == 1 else f"{node}_L{current_layer-1}_{direction[0]}_{origin}", 
                                    **edge_data)
                nodes_to_add.add(unique_node)
                get_neighbors(neighbor, current_layer + 1, direction, origin)
            else:
                # 直接添加节点和边
                edges_to_add.add(((neighbor, node) if direction == "upstream" else (node, neighbor)))
                nodes_to_add.add(neighbor)
                get_neighbors(neighbor, current_layer + 1, direction, origin)

    # 遍历初始节点列表，获取上下游邻居
    for node in node_list:
        nodes_to_add.add(node)
        get_neighbors(node, 1, "upstream", origin=node)
        get_neighbors(node, 1, "downstream", origin=node)
    
    # 添加节点和边到子图
    if not tree_structure:
        # 添加节点并复制属性
        for node in nodes_to_add:
            subgraph.add_node(node, **G.nodes[node])
        # 添加边并复制属性
        for u, v in edges_to_add:
            edge_data = G.get_edge_data(u, v)
            if edge_data:
                subgraph.add_edge(u, v, **edge_data)
    
    return subgraph

# 使用示例
if __name__ == "__main__":
    import os
    
    neo_host = neo4j_SPLC.Neo4jClient()
    exporter=Neo4jExporter(neo_host, sql_host)
    G_dict=exporter.export_supply_relations_by_year(start_year=2013, end_year=2019)
    
    output_path=r"result\YearOutput"
    for year, G in G_dict.items():
        file_path=os.path.join(output_path, f"{year}.graphml")
        nx.write_graphml(G, file_path)
        print(f"Year {year} Network: {len(G.nodes())} nodes, {len(G.edges())} edges")