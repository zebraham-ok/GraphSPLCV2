from neo4j import GraphDatabase
from tqdm import tqdm
# import os
import random,time
from .secret_manager import read_secrets_from_csv

# 从密码文件中读取
secret_file=r"API\secrets.csv"
secret_dict=read_secrets_from_csv(filename=secret_file)

local_url = secret_dict["local_neo4j_url"]
local_username = secret_dict["local_neo4j_username"]
local_password = secret_dict["local_neo4j_password"]
local_driver = GraphDatabase.driver(local_url, auth=(local_username, local_password))

remote_url = secret_dict["remote_neo4j_url"]
remote_username = secret_dict["remote_neo4j_username"]
remote_password = secret_dict["remote_neo4j_password"]
remote_driver = GraphDatabase.driver(remote_url, auth=(remote_username, remote_password))

# def execute_query(query,parameters=None, driver=local_driver):
#     with driver.session() as session:
#         result = session.run(query, parameters)
#         return [record for record in result]

class Neo4jClient():
    def __init__(self, driver=local_driver):
        self.driver=driver
    
    def execute_query(self, query, parameters=None, max_retries=3,database="neo4j"):
        attempt = 0
        while attempt < max_retries:
            try:
                with self.driver.session(database=database) as session:
                    result = session.run(query, parameters)
                    return [record for record in result]
            except Exception as e:
                attempt += 1
                if attempt < max_retries:
                    wait_time = 2 ** attempt + random.random()  # Exponential backoff with jitter
                    print(f"Attempt {attempt} failed with error {e}. Retrying in {wait_time:.2f} seconds...")
                    time.sleep(wait_time)
                else:
                    print(f"Error! All {max_retries} attempts failed. Raising the exception.")

    def Create_node(self, label, attributes, database="neo4j", merge=True, set_date=False):
        "这里使用的是MERGE方法，即：只有在新节点与旧节点完全不冲突的时候，才会合并，否则就会新创建一个同名不同属性的节点"
        
        if merge:  # 如果选择了merge方法，则优先检查一下有没有一摸一样的节点，有的话直接返回节点id
            matched_id=self.Node_exists_1st(match_attributes=attributes, label=label,database=database)
            if matched_id:
                return matched_id
        # 如果没有，或者merge是False，则进行创建节点的操作（cypher中虽然也有Merge语法，但是似乎不太好用）
        attrs = ', '.join(f"{remove_special_characters(k.replace('&','n').replace(' ','_'))}: ${remove_special_characters(k.replace('&','n').replace(' ','_'))}" for k in attributes.keys())
        query = f"CREATE (n:{label} {{{attrs}}}) return ElementId(n)"    
        result=self.execute_query(query, attributes, database=database)
        if result:
            node_id=result[0].value()
            if set_date:  # 增加了创建节点自动打上创建日期戳（一旦使用这个，MERGE就相当于全是CREATE了）
                self.execute_query("match (n) where elementid(n)=$id set n.createDate=Date()",parameters={"id":node_id})
            return node_id
        else:
            print(f"Warning! Node creation of {label}, {attributes} failed.")
        
    # 添加节点属性
    def add_properties_to_node(self, match_attributes, new_attributes,label=None, database="neo4j"):
        # 创建匹配属性的字符串
        match_attrs = ' AND '.join(f"n.{k} = ${k}" for k in match_attributes.keys())
        
        # 创建新属性的字符串
        new_attrs = ', '.join(f"n.{remove_special_characters(k.replace('&','n').replace(' ','_'))} = ${remove_special_characters(k.replace('&','n').replace(' ','_'))}" for k in new_attributes.keys())
        
        # 构建完整的Cypher查询
        if label:
            query = f"MATCH (n:{label}) WHERE {match_attrs} SET {new_attrs}"
        else:
            query = f"MATCH (n) WHERE {match_attrs} SET {new_attrs}"
        self.execute_query(query, {**match_attributes, **new_attributes}, database=database)
        return query

    def add_node_list_item(self, match_attributes, list_attr, added_item, label=None, database="neo4j"):
        "向一个列表性质的节点属性添加元素"
        match_attrs = ' AND '.join(f"n.{k} = ${k}" for k in match_attributes.keys())
        params={**match_attributes}
        params.update({"added_item":added_item})
        if label:
            query = f'''MATCH (n:{label} WHERE {match_attrs})
            SET n.{list_attr} = coalesce(n.{list_attr}, []) + $added_item
            RETURN n'''
        else:
            query = f'''MATCH (n WHERE {match_attrs})
            SET n.{list_attr} = coalesce(n.{list_attr}, []) + $added_item
            RETURN n'''
            
        self.execute_query(query, parameters=params, database=database)
        return query

    # 检查节点是否存在
    # def node_exists(self, match_attributes:dict, label=None, driver=local_driver,database="neo4j"):
    #     match_attrs = ' AND '.join(f"n.{k} = ${k}" for k in match_attributes.keys())
    #     if label:
    #         query = f"MATCH (n:{label}) WHERE {match_attrs} RETURN n IS NOT NULL AS exists"
    #     else:
    #         query = f"MATCH (n) WHERE {match_attrs} RETURN n IS NOT NULL AS exists"
    #     result = execute_query(query, {**match_attributes},driver=driver,database=database)
    #     return result

    def Node_exists(self, match_attributes:dict, label=None, database="neo4j"):
        "根据节点属性查询一个节点是否存在。可能有多个符合要求的，在列表中返回节点的id"
        match_attrs = ' AND '.join(f"n.{k} = ${k}" for k in match_attributes.keys())
        if label:
            query = f"MATCH (n:{label}) WHERE {match_attrs} RETURN elementid(n) as id"
        else:
            query = f"MATCH (n) WHERE {match_attrs} RETURN elementid(n) as id"
        result = self.execute_query(query, {**match_attributes}, database=database)
        return [i["id"].value() for i in  result]   # 返回一个list[str]

    def Node_exists_1st(self, match_attributes:dict, label=None, database="neo4j"):
        "根据节点属性查询一个节点是否存在。可能有多个符合要求的，只返回第一个节点的id"
        match_attrs = ' AND '.join(f"n.{k} = ${k}" for k in match_attributes.keys())
        if label:
            query = f"MATCH (n:{label}) WHERE {match_attrs} RETURN elementid(n) as id"
        else:
            query = f"MATCH (n) WHERE {match_attrs} RETURN elementid(n) as id"
        result = self.execute_query(query, {**match_attributes}, database=database)
        if result:  # 保证了node_exist返回的id和create_node返回的都是字符串
            return result[0].value()   # 只返回第一个的值
        else:
            return None

    def NodeId_exists(self, node_id, database="neo4j"):
        "查询一个节点id是否存在"
        query = f"MATCH (n) WHERE ElementId(n)=$node_id RETURN n IS NOT NULL AS exists"
        result = self.execute_query(query, {"node_id":node_id}, database=database)
        return result

    # 创建关系(旧)
    def Crt_rel_by_name(self, start_node_label, start_node_name, end_node_label, end_node_name, relationship_type, rel_attributes, database="neo4j"):
        "通过节点名称创建关系"
        try:
            # 检查起始节点和目标节点是否存在
            start_node_exists = self.Node_exists(start_node_label, {"name":start_node_name}, database=database)
            end_node_exists = self.Node_exists(end_node_label, {"name":end_node_name}, database=database)
            
            if not start_node_exists:
                return f"Error: Start node {start_node_label} with key {start_node_name} does not exist."
            if not end_node_exists:
                return f"Error: End node {end_node_label} with key {end_node_name} does not exist."
            
            # 创建关系
            rel_attrs = ', '.join(f"{k}: ${k}" for k in rel_attributes.keys())
            query = f"""
            CYPHER notifications=WARN
            MATCH (a:{start_node_label}) WHERE a.name = $start_node_key 
            MATCH (b:{end_node_label}) WHERE b.name = $end_node_key
            MERGE (a)-[r:{relationship_type} {{{rel_attrs}}}]->(b)
            set r.createDate=Date()
            RETURN count(r) as created_count
            """     # 注意，这里已经改为将name作为唯一标志
            parameters = {**rel_attributes, 'start_node_key': start_node_name, 'end_node_key': end_node_name}
            result = self.execute_query(query, parameters, database=database)
            
            created_count = result[0]['created_count']
            if created_count > 0:
                return "Relationship created successfully."
            else:
                return "Failed to create relationship."
        except Exception as e:
            return f"An error occurred: {e}"

    # 创建关系by id（新）
    def Crt_rel_by_id(self, start_node_id, end_node_id, relationship_type, start_node_label="", end_node_label="", rel_attributes={}, database="neo4j", set_date=False):
        "通过节点id创建关系"
        try:
            # 检查起始节点和目标节点是否存在
            start_node_exists = self.NodeId_exists(start_node_id, database=database)
            end_node_exists = self.NodeId_exists(end_node_id, database=database)
            
            if not start_node_exists:
                return f"Error: Start node {start_node_id} does not exist."
            if not end_node_exists:
                return f"Error: End node {end_node_id} does not exist."
            
            # 建立label（这样可以提升匹配的效率）
            if start_node_label:
                start_node_label=":"+start_node_label
            if end_node_label:
                end_node_label=":"+end_node_label
                
            if set_date:
                set_date_str="set r.createDate=Date()"
            else:
                set_date_str=""
                
            # 创建关系
            rel_attrs = ', '.join(f"{k}: ${k}" for k in rel_attributes.keys())
            query = f"""
            MATCH (a{start_node_label}) WHERE ElementId(a) = $start_node_id 
            MATCH (b{end_node_label}) WHERE ElementId(b) = $end_node_id
            MERGE (a)-[r:{relationship_type} {{{rel_attrs}}}]->(b)
            {set_date_str}
            RETURN count(r) as created_count"""
            parameters = {**rel_attributes, 'start_node_id': start_node_id, 'end_node_id': end_node_id}
            result = self.execute_query(query, parameters, database=database)
            
            created_count = result[0]['created_count']
            if created_count > 0:
                return f"Relationship created successfully between {start_node_id} and {end_node_id}"
            else:
                return "Failed to create relationship."
        except Exception as e:
            return f"An error occurred: {e}"
        
    def batch_merge_nodes(self, nodes, label, merge_key):
        """批量合并节点"""
        query = f'''
            UNWIND $nodes AS node
            MERGE (n:{label} {{{merge_key}: node.{merge_key}}})
            SET n += apoc.map.clean(node, ["{merge_key}"], [])
            RETURN elementId(n) AS id
        '''
        return self.execute_query(query, {"nodes": nodes})
    
    def batch_create_relationships(self, rels):
        """批量创建关系"""
        query = '''
            UNWIND $rels AS rel
            MATCH (a) WHERE elementId(a) = rel.source
            MATCH (b) WHERE elementId(b) = rel.target
            CALL apoc.merge.relationship(
                a, 
                rel.type,
                {},
                rel.props,
                b,
                {}
            ) YIELD rel AS r
            RETURN count(r)
        '''
        return self.execute_query(query, {"rels": rels})

        # 返回一个随机的公司
    def sample_node_by_label(self, label="Company", sample_size=1, database="neo4j"):
        query = f"""
        MATCH (n:{label})
        RETURN n
        ORDER BY rand()
        LIMIT $sample_size
        """
        parameters = {'sample_size': sample_size}
        return self.execute_query(query, parameters, database=database)

    def Update_node(self, update_attr_dict:dict, identifier_value, identifier_key="elementid", label=None, database="neo4j"):
        set_attrs = ', '.join(f"n.{k} = ${k}" for k in update_attr_dict.keys())
        "使用elementid或者某一属性为索引，更新一个节点的属性"
        
        if label:
            label_str=f": {label}"
        else:
            label_str=""
        
        if identifier_key=="elementid":
            query=f'''
                MATCH (n{label_str}) where elementid(n)='{identifier_value}'
                SET {set_attrs}
                RETURN n
            '''
            parameters = update_attr_dict
        else:
            query = f"""
                MATCH (n{label_str} {{{identifier_key}: ${identifier_key}_value}})
                SET {set_attrs}
                RETURN n
            """
            parameters = {**update_attr_dict, f'{identifier_key}_value': identifier_value}
        # print(query)
        return self.execute_query(query, parameters, database=database)

    def reset_node_label(self, old_label, new_label, node_name=None, node_id=None, database="neo4j"):
        "重新设置节点的Label，可以根据node_name进行设定，也可以根据node_id进行设定（id优先）"
        if old_label==new_label:
            return
        if node_id:
            query=f'''match (n) where elementid(n)='$id'
            set n:{new_label}
            remove n:{old_label}
            '''
            self.execute_query(query=query, parameters={"id":node_id}, database=database)
        elif node_name:
            query=f'''match (n) where n.name=$name
            set n:{new_label}
            remove n:{old_label}
            '''
            self.execute_query(query=query, parameters={"name":node_name}, database=database)
        else:
            print("You must provide node_name or node_id")

    def DeleteNode_by_attr(self, label, identifier_key, identifier_value, database="neo4j"):
        "根据一个属性的值匹配节点，之后将其删除，返回删除的节点个数。如果节点有连边，也会一并删除"
        query = f"MATCH (n:{label} {{{identifier_key}: ${identifier_key}_value}}) DETACH DELETE n return count(n)"
        parameters = {f'{identifier_key}_value': identifier_value}
        result=self.execute_query(query, parameters, database=database)
        return result

    def DeleteNode_by_name(self, nodename,database="neo4j"):
        attempt = 0
        max_retries=3
        while attempt < max_retries:
            try:
                query = "MATCH (n {name: $nodeName}) DETACH DELETE n"
                with self.driver.session(database=database) as session:
                    result = session.run(query,nodeName=nodename)
                    return [record for record in result]
            except Exception as e:
                attempt += 1
                if attempt < max_retries:
                    wait_time = 2 ** attempt + random.random()  # Exponential backoff with jitter
                    print(f"Attempt {attempt} failed with error {e}. Retrying in {wait_time:.2f} seconds...")
                    time.sleep(wait_time)
                else:
                    print(f"Error! All {max_retries} attempts failed. Raising the exception.")
                    return

    def DeleteNode_by_id(self, node_id, database="neo4j"):
        "根据Id匹配节点，之后将其删除，返回删除的节点个数，也会一并删除"
        query = f"MATCH (n) where ElementId(n)=$node_id DETACH DELETE n return count(n)"
        parameters = {"node_id": node_id}
        result=self.execute_query(query, parameters, database=database)
        return result  # 返回删除的节点个数，应该就是一个count(1)类

    def DeleteRel_by_id(self, rel_id, database="neo4j"):
        "根据Id匹配连边，之后将其删除，返回删除的节点个数，也会一并删除"
        query = f"MATCH (n)-[r]-(m) where ElementId(r)=$rel_id DELETE r return count(r)"
        parameters = {"rel_id": rel_id}  # 查询连边时，必须连带查询节点
        result=self.execute_query(query, parameters, database=database)
        return result  # 返回删除的节点个数，应该就是一个count(1)类

    # 示例: 删除公司
    # delete_node('Company', 'Id', 'Tech Innovations')
    def delete_relationship(self, start_node_label, start_node_key, end_node_label, end_node_key, relationship_type, rel_attributes=None, database="neo4j"):
        match_clause = f"MATCH (a:{start_node_label})-[r:{relationship_type}]->(b:{end_node_label})"
        where_clause = f"a.Id = $start_node_key AND b.Id = $end_node_key"
        # 如果指定了关系属性，增加属性匹配条件
        if rel_attributes:
            rel_conditions = ' AND '.join(f"r.{k} = ${k}" for k in rel_attributes.keys())
            where_clause += ' AND ' + rel_conditions

        query = f"{match_clause} WHERE {where_clause} DELETE r"
        parameters = {**rel_attributes, 'start_node_key': start_node_key, 'end_node_key': end_node_key} if rel_attributes else {'start_node_key': start_node_key, 'end_node_key': end_node_key}
        self.execute_query(query, parameters, database=database)
        return query

    def check_relation_existence(self, node_id, relation_label, database="neo4j"):
        "检查一个节点是否已经有某一种连边了"
        query = """
        MATCH (n {id: $node_id})-[:%s]->()
        RETURN count(*)
        """%relation_label

        # 执行查询
        with self.driver.session(database=database) as session:
            result = session.run(query, node_id=node_id)
            count = result.single()[0]  # 获取计数结果
        return count

    # 查找连边（可以指定只查找某一种连边，如Name）
    def get_node_relationships(self, node_id, rel_type="", database="neo4j"):
        "根据节点id，获取一个节点的所有关系，返回两个值，一个in，一个out"
        with self.driver.session(database=database) as session:
            if not rel_type:
                query_in = """
                MATCH (n {Id: $node_id})<-[r]-(m)
                RETURN collect(m.Id) as incoming_nodes
                """
                query_out = """
                MATCH (n {Id: $node_id})-[r]->(m)
                RETURN collect(m.Id) as outgoing_nodes
                """
                incoming_result = session.run(query_in, node_id=node_id).single()["incoming_nodes"]
                outgoing_result = session.run(query_out, node_id=node_id).single()["outgoing_nodes"]
            else:
                query_in = f"""
                MATCH (n {{Id: $node_id}})<-[r:{rel_type}]-(m)
                RETURN collect(m.Id) as incoming_nodes
                """
                query_out = f"""
                MATCH (n {{Id: $node_id}})-[r:{rel_type}]->(m)
                RETURN collect(m.Id) as outgoing_nodes
                """
                incoming_result = session.run(query_in, node_id=node_id).single()["incoming_nodes"]
                outgoing_result = session.run(query_out, node_id=node_id).single()["outgoing_nodes"]
            return incoming_result, outgoing_result

    # 查询一个节点的所有连边，并附带连边节点的属性
    def get_node_rel_info_by_name(self, node_name, database="neo4j"):
        "根据节点名称，获取一个节点的所有关系，返回两个值，一个in，一个out"
        with self.driver.session(database=database) as session:
            # 查询进入当前节点的所有关系和关系的属性
            query_in = """
            MATCH (m)-[r]->(n:Company {Id: $node_id})
            RETURN collect({source_id: m.Id, relationship_type: type(r), properties: properties(r)}) AS incoming_relations
            """
            # 查询从当前节点出去的所有关系和关系的属性
            query_out = """
            MATCH (n:Company {Id: $node_id})-[r]->(m)
            RETURN collect({target_id: m.Id, relationship_type: type(r), properties: properties(r)}) AS outgoing_relations
            """
            # 执行查询并获取结果
            incoming_result = session.run(query_in, node_name=node_name).single()["incoming_relations"]
            outgoing_result = session.run(query_out, node_name=node_name).single()["outgoing_relations"]
            return incoming_result, outgoing_result

    def find_duplicate_company(self, database="neo4j"):
        # 查找具有相同Name的节点
        query_name = """
        MATCH (n:Company)
        WHERE n.Name IS NOT NULL
        WITH n.Name AS name, COLLECT(n) AS nodes
        WHERE SIZE(nodes) > 1
        RETURN nodes
        """
        duplicate_name_nodes = self.execute_query(query_name, database=database)

        # 查找具有相同Id的节点
        query_id = """
        MATCH (n:Company)
        WHERE n.Id IS NOT NULL
        WITH n.Id AS id, COLLECT(n) AS nodes
        WHERE SIZE(nodes) > 1
        RETURN nodes
        """
        duplicate_id_nodes = self.execute_query(query_id, database=database)

        return duplicate_name_nodes + duplicate_id_nodes

    def find_name_duplicate_nodes(self, database="neo4j"):
        # 查找具有相同Name的节点
        query_name = """
        MATCH (n:Entity)
        WHERE n.name IS NOT NULL
        WITH n.name AS name, COLLECT(n) AS nodes
        WHERE SIZE(nodes) > 1
        RETURN nodes
        """
        
        duplicate_name_nodes = self.execute_query(query_name, database=database)

        return duplicate_name_nodes

    # 查询一个节点的所有连边，并附带连边节点的属性
    def get_node_rel_info_byId(self, node_id, database="neo4j"):
        with self.driver.session(database=database) as session:
            # 查询进入当前节点的所有关系和关系的属性
            query_in = """
            MATCH (m)-[r]->(n)
            where ElementId(n)=$node_id
            RETURN collect({source_id: ElementId(m), relationship_id:ElementId(r), relationship_type: type(r), properties: properties(r)}) AS incoming_relations
            """
            # 查询从当前节点出去的所有关系和关系的属性
            query_out = """
            MATCH (n)-[r]->(m)
            where ElementId(n)=$node_id
            RETURN collect({target_id: ElementId(m), relationship_id:ElementId(r), relationship_type: type(r), properties: properties(r)}) AS outgoing_relations
            """
            # 执行查询并获取结果
            incoming_result = session.run(query_in, node_id=node_id).single()["incoming_relations"]
            outgoing_result = session.run(query_out, node_id=node_id).single()["outgoing_relations"]
            return incoming_result, outgoing_result

    # 针对查询到的节点进行合并
    def merge_duplicate_nodes(self, nodes_to_merge, database="neo4j"):
        for node_group in tqdm(nodes_to_merge):               # merge_key指根据那个属性的值进行合并
            attributes = {}
            all_incoming = []
            all_outgoing = []
            
            # 提取所有节点的属性和边
            for node in node_group[0]:  # 这个[0]没有什么意义
                node_dict = dict(node)
                for key, value in node_dict.items():
                    if key not in attributes:  # 如果之前没有，就填写
                        attributes[key]=value
                    elif str(value) not in str(attributes[key]):  # 如果之前有这个属性，但没有这个取值，就用逗号连接
                        attributes[key]=",".join([str(attributes[key]),str(value)])
                        
                # 获取边
                incoming, outgoing = self.get_node_rel_info_byId(node.element_id, database=database)
                all_incoming.extend(incoming)
                all_outgoing.extend(outgoing)

            # 删除旧节点
            nodes_label_set=set()
            for node in node_group[0]:  # merge_key='name'
                node_label_list=list(node.labels)  # 从<Node element_id='4:9628c7e9-c112-4157-a0b0-bd43ceb10e24:16' labels=frozenset({'Company'}) properties={'name': '英飞凌科技股份公司'}>这样的对象中提取labels
                nodes_label_set.update(node_label_list)
                # delete_node(node_label_list[0], merge_key, node[merge_key], driver=driver)  # 这里可能不只是Company！！！要改
                self.DeleteNode_by_id(node.element_id, database=database)
            # 创建新节点
            # new_node_id = attributes[merge_key]  # 使用最短的Id
            used_label="Entity"
            for label in ["Technology","Factory","Location","CompanyType","CompanyObj","Company","Prodcut","Person","Industry","Event","Market","Service","Organization","Name","Entity"]:
                # 优先级方案应该有所调整，改为少数服从多数方案！！！
                if label in nodes_label_set:
                    used_label=label
                    break
                
            # 创建一个新节点之后，要立刻获取这个节点的id
            new_node_id=self.Create_node(used_label, attributes, database=database)
            
            # 连接所有边
            for in_rel_info in all_incoming:
                if "source_id" in in_rel_info:
                    # r=create_relationship(list(in_rel_info.labels)[0], in_rel_info["source_id"], used_label, new_node_id, relationship_type=in_rel_info["relationship_type"],rel_attributes=in_rel_info["properties"], driver=driver)
                    r=self.Crt_rel_by_id(start_node_id=in_rel_info["source_id"],end_node_id=new_node_id,relationship_type=in_rel_info["relationship_type"],rel_attributes=in_rel_info["properties"], database=database)
                    # print(r)
                else:
                    print(in_rel_info)
            for out_rel_info in all_outgoing:
                if "target_id" in out_rel_info:
                    r=self.Crt_rel_by_id(start_node_id=new_node_id,end_node_id=out_rel_info["target_id"], relationship_type=out_rel_info["relationship_type"],rel_attributes=out_rel_info["properties"], database=database)
                    # print(r)
                else:
                    print(out_rel_info)
    # 示例: 创建交易关系
    # create_relationship('Company', 'Tech Innovations', 'Company', 'Global Corp', 'TRADES_WITH', {'product': 'Electronics', 'amount': 500})

    # 这个是更新版本，相比于node_exists，它能够返回节点的信息
    def query_NodeInfoDict_list_by_attr(self, match_attributes:dict,label=None, database="neo4j"):
        # 可以不限label地进行检索
        # query = f"MATCH (n:{label}) WHERE n.Id = $Id RETURN n"
        match_attrs = ' AND '.join(f"n.{k} = ${k}" for k in match_attributes.keys())
        if label:
            query = f"MATCH (n:{label}) WHERE {match_attrs} RETURN n"
        else:
            query = f"MATCH (n) WHERE {match_attrs} RETURN n"
        results = self.execute_query(query, parameters={**match_attributes}, database=database)
        # 检查返回的结果数量，正确应只有一个节点
        if len(results) == 1:
            return [results[0]["n"]]  # 这个n代指query中的n
        elif len(results) > 1:
            print(f"Warning: More than one node with {match_attributes} found.")
            return [i["n"] for i in results]
        else:
            print("neo4j_SPLC.query_NodeInfoDict_list_by_attr Error: No nodes found.")
            return []
        
    
    def pick_one_node(self, useful_industry:list, useful_business_model:list, database="neo4j",min_degree=1,skip_searched=True,searched_time_mark="last_searched",):
        '''
            主要根据原来节点的属性，对于IDM，Foundry，设备厂商，wafer厂商、封测，这些重要的公司类型，
            以及Industry选取要检索的公司（尤其是那些连边数量很少的公司），进行关键词检索
            抽取一个节点，看是否具有所需的属性、看是否已经足够多信息、看之前是否已经检索
            如果符合条件，检索，不符合，下一个
        '''
        while True:
            count_query=f"MATCH (n:Company) WHERE apoc.node.degree(n) >= {min_degree} RETURN count(n) AS totalNodes"
            count=self.execute_query(count_query, database=database)
            if count:
                break
            elif min_degree>1:  # 如果没有合适的，就适当调低中心度的要求
                print(f"no aviliable node for search which degree is above {min_degree}")
                min_degree-=1
            else:
                print(f"no aviliable node for search which degree is above {min_degree}")
                return None,None

        flag=False  # 表示还没找到合适的节点
        while not flag:
            number_of_nodes=count[0]["totalNodes"]
            random_node_index=random.randint(0,number_of_nodes-1)
            select_query=f"match (n:Company) WHERE apoc.node.degree(n) >= {min_degree} return n as node_dict,elementid(n) as node_id, apoc.node.degree(n) as degree SKIP {random_node_index} limit 1"
            selected_node=self.execute_query(select_query, database=database)
            if selected_node:
                returned_data=selected_node[0].data()
                node_dict=returned_data["node_dict"]  # 随机选择的一个节点
                node_id=returned_data["node_id"]
                node_degree=returned_data["degree"]
                if skip_searched:
                    if searched_time_mark in node_dict:  # 如果这是一个已经搜索过的节点，就不要再搜索了
                        print(node_dict["name"]," ",node_dict[searched_time_mark],end="")
                        if node_dict[searched_time_mark]:  # 一旦有这个标记
                            print("already searched")
                            continue
                
                if "industry" in node_dict:
                    temp_indus_str=node_dict["industry"]
                    if isinstance(temp_indus_str,str):
                        for temp_indus in temp_indus_str.split(","):
                            if temp_indus in useful_industry:
                                flag=True
                                break
                if "business_model" in node_dict:
                    temp_bm_str=node_dict["business_model"]
                    if isinstance(temp_bm_str,str):
                        for temp_bm in temp_bm_str.split(","):
                            if temp_bm in useful_business_model:
                                flag=True
                                break
                            
        print(f"selected a node with degree {node_degree}")
        return node_id,node_dict
    
    def vector_search(self, Label: str, query , attr, attr_value = "", database="neo4j", limit=10):
        """
        基于向量索引的相似性搜索（Neo4j 5.19+）
        :attr: 要返回的属性
        :param Label: 节点标签，如 "Section"，这决定了index的使用，如section_qwen_embedding_index
        :param attr: 向量属性名，如 "embedding"
        :param query_vector: 查询向量（需与索引维度一致）
        :return: 包含(elementid, 文本, 相似度分数)的列表
        """
        # 动态生成索引名（需提前创建）
        if Label not in ["Section","Article","ProductCategory"]:
            index_name = "entityobj_qwen_embedding_index"
        else:
            index_name = f"{Label.lower()}_qwen_embedding_index"
        
        if attr_value:  # 如果是要求包含某一内容的检索
            cypher = f"""
                CALL db.index.vector.queryNodes($index_name, $top_k, $query_vector)
                YIELD node, score
                where node.{attr} contains '{attr_value}' and '{Label}' in labels(node) and (node.rubbish <> true or node.rubbish is null)
                RETURN elementid(node) AS id, 
                    node.{attr} AS text, 
                    score
                ORDER BY score DESC
                LIMIT 100
            """
        else:
            cypher = f"""
                CALL db.index.vector.queryNodes($index_name, $top_k, $query_vector)
                YIELD node, score
                where '{Label}' in labels(node) and (node.rubbish <> true or node.rubbish is null)
                RETURN elementid(node) AS id, 
                    node.{attr} AS text, 
                    score
                ORDER BY score DESC
                LIMIT 100
            """
        
        # 如果给的是文本，则需要先导入文本嵌入
        if isinstance(query, str):
            from API.ai_ask import get_qwen_embedding
            query=get_qwen_embedding(query)
        
        params = {
            "index_name": index_name,
            "top_k": int(limit)*10,  # 近似最近邻数量
            "query_vector": query,
            "text_property": attr,  # 需要返回的文本属性名
            "limit": limit  # 实际返回的数量
        }
        
        # print(cypher)
        results = self.execute_query(cypher, params, database=database)
        return [(record["id"], record["text"], record["score"]) for record in results]
  
def remove_special_characters(s):
    "去除不能作为node属性的字符"
    chars_to_remove = "+-×÷/\\|*%$&#@!、"
    return ''.join(c for c in s if c not in chars_to_remove)

def make_safe_label(label):
    "把不能作为neo4j中的label的字符去掉"
    return label.replace(" ","_").replace("-","_").replace("&","n").replace("%","percent").replace("\\","_").replace(":","_").replace(".","_").replace("[","_").replace("(","_").replace("]","_").replace(")","_").replace("__","_").replace("__","_").replace("*","").replace("=","_")

# 示例: 查询公司
# query_nodes('Company', {'location': 'Silicon Valley'})

def get_name(node_dict):  # 从变量字典中获取变量名
    if "Name" in node_dict:
        node_name=node_dict["Name"]
    else:
        node_name=node_dict["Id"]
    return node_name

def get_en_name(node_dict):  # 从变量字典中获取变量名
    if "en-name" in node_dict:
        node_name=node_dict["en-name"]
    elif "Name" in node_dict:
        node_name=node_dict["Name"]
    else:
        node_name=node_dict["Id"]
    return node_name


                

# 要给出自动爬取的方案
# 根据一些node+芯片/chip 在互联网上的检索值，来确定他们与芯片之间相关性的强弱?
# 总之要有一个判断方法，决定什么样的节点可以作为下一步的检索对象

