import weaviate
import uuid
import json

# 版本3目前不是最新版，但是版本4与另一个必要的库有冲突，文档：https://weaviate-python-client.readthedocs.io/en/v3.11.0/index.html

class WeaviateClient:
    def __init__(self, client_url="http://localhost:8080", web_key=""):
        """
        初始化 WeaviateClient 对象并连接到指定的 Weaviate 服务。
        
        参数:
        - client_url: Weaviate 服务 URL
        """
        if web_key:
            "这个似乎目前不好用"
            self.client = weaviate.Client(
                url=client_url,
                auth_client_secret=weaviate.auth.AuthApiKey(api_key=web_key),
            )
        else:
            self.client = weaviate.Client(client_url)
        
        # 检查 Weaviate 服务是否可用
        try:
            self.client.is_ready()
            print(f"成功连接到 Weaviate 服务: {client_url}")
        except Exception as e:
            print(f"无法连接到 Weaviate 服务: {e}")
            raise
    
    def create_schema_from_json(self, schema_json):
        "这个函数只能用于初次创建，不能用于修改"
        self.client.schema.create(schema_json)
    
    def create_class(self, class_name, properties):
        """
        向 Weaviate schema 中添加一个新的 class。
        
        参数:
        - class_name: 要创建的类的名称
        - properties: 类的属性定义，每个属性为字典形式，包含名称和数据类型
        """
        schema = {
            "classes": [
                {
                    "class": class_name,
                    "properties": properties
                }
            ]
        }
        
        # 如果 class 已经存在，避免重复创建
        if class_name not in [cls["class"] for cls in self.client.schema.get()["classes"]]:
            self.client.schema.create(schema)
            print(f"成功创建 class: {class_name}")
        else:
            print(f"Class {class_name} 已存在。")
            
    def get_class_instance_counts(self):
        """
        获取每个类（Class）下的实例数量统计
        
        返回:
        - 字典，键为类名，值为对应的实例数量（无法获取时为None）
        """
        counts = {}
        # 获取所有已存在的类
        schema = self.client.schema.get()
        classes = schema.get('classes', [])
        
        for cls in classes:
            class_name = cls['class']
            # 构造聚合查询
            query = f'''{{
                Aggregate {{
                    {class_name} {{
                        meta {{
                            count
                        }}
                    }}
                }}
            }}'''
            
            # try:
            result = self.client.query.raw(query)
            # 检查是否有错误
            if 'errors' in result:
                print(f"查询类 {class_name} 的数量时出错: {result['errors']}")
                counts[class_name] = None
                continue
            
            # 解析结果
            aggregate_data = result.get('data', {}).get('Aggregate', {})
            class_data = aggregate_data.get(class_name, [])
            if class_data:
                class_meta=class_data[0].get('meta', {})
                count = class_meta.get('count', 0)
            counts[class_name] = count
            # except Exception as e:
            #     print(f"执行聚合查询时发生异常: {e}")
            #     counts[class_name] = None
        
        return counts
    
    def create_data_object(self, class_name, data):
        """
        创建一个数据对象并将其添加到 Weaviate 中。
        
        参数:
        - class_name: 数据对象所属的类名
        - data: 数据字典，包含所有要存储的字段
        """
        try:
            # 自动生成 UUID，如果数据中没有 UUID 字段
            if 'uuid' not in data:
                node_uuid=str(uuid.uuid4())
                data['uuid'] = node_uuid
            # 虽然如果不指定uuid的话，程序也会赋予一个，但是那样的话，uuid仅出现在additional当中，无法便捷地返回，因此这里会直接将uuid加入到主字段当中
            self.client.data_object.create(data, class_name=class_name, uuid=node_uuid)
            return node_uuid
        except Exception as e:
            print(f"创建数据对象失败: {e}")
            return None
    
    def query_graphql(self, query):
        """
        执行 GraphQL 查询。
        
        参数:
        - query: GraphQL 查询字符串
        
        返回:
        - 查询结果
        """
        try:
            result = self.client.query.raw(query)
            return result
        except Exception as e:
            print(f"GraphQL 查询失败: {e}")
            return None

    def search_class_objects(self, class_name, property, query_string, operator="Contains"):
        """
        在指定的类（Class）中查找属性等于或包含某一字符串的对象。
        支持以下两种查询方式：
        1. 严格等于搜索：属性的值必须严格等于 query_string。
        2. 包含搜索：属性的值包含 query_string（适用于文本类型属性）。
        
        参数:
        - class_name: 要查询的类名称（e.g., "Product"）
        - property: 要查询的属性名称（e.g., "name"）
        - query_string: 查询的关键字（e.g., "laptop"）
        - include_subclasses: 是否包含子类（默认为 False）
        
        返回:
        - 匹配对象的列表
        """
        # 构造查询条件
        where_filter = {
            "path": [property],
            "operator": operator,
            "valueString": query_string
        }

        # 执行查询
        query_results = (
            self.client.query
            .get(class_name, properties=["*"])
            .with_where(where_filter)
            .with_additional(["id", "beacon"])
            .do()
        )

        # 提取结果
        results = query_results.get('data', {}).get('Get', {}).get(class_name, [])

        # 返回匹配的对象列表
        return results

    # 未经验证
    def connect_data_objects(self, from_class_name, to_class_name, from_uuid, to_uuid, property_name):
        """
        在两个数据对象之间建立连接（通过引用）。
        
        参数:
        - class_name: 数据对象所属的类名
        - from_uuid: 起始数据对象的 UUID
        - to_uuid: 目标数据对象的 UUID
        - from_property: 起始对象中用于建立连接的属性
        - to_property: 目标对象中用于建立连接的属性
        """
        # 建立引用关系
        '''from_uuid: str,
        from_property_name: str,
        to_uuid: str,
        from_class_name: str | None = None,
        to_class_name: str | None = None,
        consistency_level: ConsistencyLevel | None = None,
        tenant: str | None = None'''
        
        try:
            self.client.data_object.reference.add(from_uuid=from_uuid, from_class_name=from_class_name, to_class_name=to_class_name, from_property_name=property_name, to_uuid=to_uuid)
            # print(f"成功建立连接: {from_uuid} -> {to_uuid}")
        except Exception as e:
            print(f"建立连接失败: {e}")
    
    def delete_class(self, class_name):
        """
        删除一个 class。
        
        参数:
        - class_name: 要删除的类名
        """
        try:
            self.client.schema.delete_class(class_name)
            print(f"Class {class_name} 已成功删除！")
        except Exception as e:
            print(f"删除 Class {class_name} 失败: {e}")
    
    def delete_data_object(self, class_name, uuid):
        """
        删除指定的 data object。
        
        参数:
        - class_name: 数据对象所属的类名
        - uuid: 要删除的对象的 UUID
        """
        try:
            self.client.data_object.delete(uuid, class_name)
            print(f"数据对象 {uuid} 已成功删除！")
        except Exception as e:
            print(f"删除数据对象失败: {e}")
    
    def search_text_like(self, class_name, field, query, top_k=5, field_list=["uuid"]):
        "不使用相似度，只使用文本匹配（可以用*表示任意字符）"
        try:
            result = self.client.query.get(class_name, field_list).with_where({
                "path": [field],
                "operator": "Like",
                "valueString": query
            }).with_limit(top_k).do()
            return result
        except Exception as e:
            print(f"Like检索失败: {e}")
            return None
    
    def search_by_text_similarity(self, class_name, query, field_list=["uuid"], top_k=5):
        """
        按照文本相似度进行检索。
        
        参数:
        - class_name: 数据对象所属的类名
        - text: 要查询的文本
        - top_k: 返回最相似的 top_k 个结果
        - field_list: 要返回的属性（如果不写则默认返回全部）
        """
        if not field_list:
            schema = self.client.schema.get(class_name)
            field_list = [i['name'] for i in schema["properties"] if i['indexSearchable']]

        try:
            result = self.client.query.get(class_name, field_list).with_near_text({
                "concepts": [query]
            }).with_limit(top_k).do()
            return result
        except Exception as e:
            print(f"文本相似度检索失败: {e}")
            return None
    
    def search_by_text_similarity_rerank(self, class_name, query, rerank_property, rerank_query='', field_list=["uuid"], top_k=5, contain_str=None, contain_field=None):
        """
        按照文本相似度进行检索，并使用指定属性进行重排序。请确保已经启动了Weaviate的rerank模块

        参数:
        - class_name: 数据对象所属的类名
        - query: 要查询的文本
        - rerank_property: 用于重排序的属性名称
        - top_k: 返回最相似的 top_k 个结果
        - field_list: 要返回的属性（如果不写则默认返回全部）
        """
        if not field_list:
            schema = self.client.schema.get(class_name)
            field_list = [i['name'] for i in schema["properties"] if i['indexSearchable']]
            
        if not rerank_query:
            rerank_query=query

        # 由于.with_addition的方法似乎不好使，只能用构建GraphQL查询字符串的方法，又因为f模式不允许\n出现，就只能这样写了
        gql = f'''{{
            Get {{
                {class_name}(
                    nearText: {{
                        concepts: ["{query}"]
                    }},'''
        
        if contain_str is not None and contain_field is not None:
            gql += f'''
            where: {{
                path: ["{contain_field}"],
                operator: Like,
                valueString: "{contain_str}"
            }}, '''
        
        gql+=f'''            limit: {top_k}
                ) {{
        '''
        for field in field_list:
            gql+=field
            gql+="\n"
        gql+=f'''
                    _additional {{
                        distance
                        rerank(
                            property: "{rerank_property}"
                            query: "{rerank_query}"
                        ) {{
                            score
                        }}
                    }}
                }}
            }}
        }}'''

        try:
            result = self.client.query.raw(gql)
            return result
        except Exception as e:
            print(f"高级文本相似度检索失败: {e}")
            return None
        
    # 这个函数可能有问题，但是还是要解决，毕竟对于企业实体来说，完全包含名称这种检索还是很必要的，不能只用Equal呀
    def search_by_text_similarity_with_move(self, class_name, query, field_list=["uuid"], top_k=5, move_to=None, move_away_from=None, contain_str=None, contain_field=None):
        """
        高级文本相似度检索，允许 moveTo、moveAwayFrom 以及必须包含某字样，并支持程度控制。
        
        参数:
        - class_name: 数据对象所属的类名
        - concepts: list，要查询的文本概念列表
        - top_k: int，返回最相似的 top_k 个结果
        - move_to: dict，想要靠近的概念及其力度（例如：{"concepts": ["高级工程师"], "force": 0.8}）
        - move_away_from: dict，想要远离的概念及其力度（例如：{"concepts": ["初级工程师"], "force": 0.6}）
        - field_list: str，要返回的属性（如果不写则默认返回全部）
        - contain_field: str，要求哪一个属性必须包含contain_str（可选）
        - contain_str: list，必须包含的文本列表（可选），前后加*表示前后可以还有别的字符
        """
        if not field_list:
            schema = self.client.schema.get(class_name)
            field_list = [i['name'] for i in schema["properties"] if i['indexSearchable']]
        
        # 构建 nearText 查询部分
        near_text_query = {
            "concepts": query,
        }
        
        if move_to is not None:
            near_text_query["moveTo"] = {"concepts": move_to.get("concepts"), "force": move_to.get("force", 0.5)}
        if move_away_from is not None:
            near_text_query["moveAwayFrom"] = {"concepts": move_away_from.get("concepts"), "force": move_away_from.get("force", 0.5)}
        
        # 如果提供了 must_contain，则添加过滤条件
        where_filter = {}
        if contain_str is not None and contain_field is not None:
            where_filter = {
                "path": [contain_field],  # 假设 'description' 字段是我们要检查的内容字段
                "operator": "Like",
                "valueString": contain_str
            }

        try:            
            # 如果有 where 条件，则应用它
            if where_filter:
                result = self.client.query.get(class_name, field_list).with_near_text(near_text_query).with_where(where_filter).with_limit(top_k)
            else:
                result = self.client.query.get(class_name, field_list).with_near_text(near_text_query).with_limit(top_k)
            
            result = result.do()
            return result
        except Exception as e:
            print(f"高级文本相似度检索失败: {e}")
            return None
    
    # def entity_recognition(self,class_name,find_entity_from_attr,more_atrr=["uuid"],where_contiton={},limit=100,certainty=0.7):
    #     "进行附带命名实体识别的查询"
    #     attr_list=["_additional {tokens ( properties: [\"%s\"], certainty: %f) {entity property word certainty startPosition endPosition}}"%(find_entity_from_attr,certainty)]
    #     attr_list=attr_list+more_atrr
        
    #     if not where_contiton:
    #         result = (
    #             self.client.query
    #             .get(class_name, attr_list)
    #             .with_limit(limit)
    #             .do()
    #         )
    #     else:
    #         result = (
    #             self.client.query
    #             .get(class_name, attr_list)
    #             .with_where(where_contiton)
    #             .with_limit(limit)
    #             .do()
    #         )
    #     return result
    def entity_recognition(self, class_name, find_entity_from_attr, more_attr=["uuid"], where_condition={}, limit=100, certainty=0.5):
        """进行附带命名实体识别的查询（基于位置直接截取）"""
        # 确保包含原始文本字段（关键修改1）
        if find_entity_from_attr not in more_attr:
            more_attr = [find_entity_from_attr] + more_attr

        attr_list = [
            "_additional {tokens (properties: [\"%s\"], certainty: %f) {entity property certainty startPosition endPosition}}" % (
                find_entity_from_attr, certainty)
        ]
        attr_list += more_attr

        query = self.client.query.get(class_name, attr_list).with_limit(limit)
        if where_condition:
            query = query.with_where(where_condition)
        result = query.do()

        # 处理结果（关键修改2）
        processed = []
        sections = result.get('data', {}).get('Get', {}).get(class_name, [])
        # print(sections)
        for section in sections:
            # 提取基础字段
            temp_data = {attr: section.get(attr) for attr in more_attr}
            
            # 获取原始文本内容
            raw_text = section.get(find_entity_from_attr, "")
            
            # 合并实体片段
            entities = []
            current_entity = None
            for token in section.get('_additional', {}).get('tokens', []):
                entity_type = token['entity'].split('-')[-1]  # 取B/I后的类型
                prefix = token['entity'].split('-')[0]  # B/I前缀
                
                # 处理B开头的实体
                if prefix == 'B':
                    if current_entity:
                        entities.append(current_entity)
                    current_entity = {
                        'type': entity_type,
                        'start': token['startPosition'],
                        'end': token['endPosition'],
                        'certainties': [token['certainty']]
                    }
                # 处理I开头的连续实体
                elif prefix == 'I' and current_entity and current_entity['type'] == entity_type:
                    current_entity['end'] = token['endPosition']
                    current_entity['certainties'].append(token['certainty'])
                else:  # 出现了无头实体的错误，跳过当前Entity
                    if current_entity:
                        entities.append(current_entity)
                    current_entity = None

            # 添加最后一个实体
            if current_entity:
                entities.append(current_entity)

            # 从原始文本截取内容（关键修改3）
            temp_data["entities"] = [{
                "type": e['type'],
                "content": raw_text[e['start']:e['end']],
                "avg_certainty": sum(e['certainties'])/len(e['certainties']),
                "start": e['start'],
                "end": e['end']
            } for e in entities]

            processed.append(temp_data)
        
        # 转换为 {uuid: entities} 格式（新增功能）
        return processed

# 示例用法
if __name__ == "__main__":
    client_url = "http://localhost:8080"  # 替换为你的 Weaviate 服务 URL
    weaviate_client = WeaviateClient(client_url)

    # result = weaviate_client.advanced_search_by_text_similarity(
    #     class_name="ProductCategory", 
    #     concepts=["建筑"], 
    #     move_to={"concepts": ["工程"], "force": 0.5}, 
    #     move_away_from={"concepts": ["服务"], "force": 0.5}, 
    #     contain_str=["工程"],
    #     contain_field="categoryName"
    # )
    # print(json.dumps(result, indent=2,ensure_ascii=False))
    

    # result=weaviate_client.entity_recognition(class_name="Section",find_entity_from_attr="content",limit=20,more_attr=["uuid","content"],certainty=0.3)
    # for info_dict in result:
    #     for key,item in info_dict.items():
    #         if isinstance(item,list):
    #             for i in item:
    #                 print(i)
    #         else:
    #             print(key,": ",item)
    weaviate_client.get_class_instance_counts()