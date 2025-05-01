import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

from tqdm import tqdm
import API.neo4j_SPLC
remote_url = "neo4j://98j3o73350.goho.co:29230"
remote_username = "neo4j"
remote_password = "splcsplc"
remote_driver = API.neo4j_SPLC.GraphDatabase.driver(remote_url, auth=(remote_username, remote_password))
neo4j_host=API.neo4j_SPLC.Neo4jClient(driver=remote_driver)

# 定义一个函数来处理每个重复实体
def process_dup_entity(record):
    try:
        name = record[0][0]["name"]
        all_incoming = []
        all_outgoing = []

        # 获取所有同名实体的 ID
        all_same_name_entity = neo4j_host.execute_query(
            query='''
                MATCH (n:Entity) WHERE n.name=$name
                RETURN elementid(n) AS id
            ''',
            parameters={"name": name}
        )

        node_id_list = [i["id"] for i in all_same_name_entity]

        # 收集所有关系信息
        for same_name_node_id in node_id_list:
            incoming, outgoing = neo4j_host.get_node_rel_info_byId(same_name_node_id)
            all_incoming.extend(incoming)
            all_outgoing.extend(outgoing)

        # 处理入边关系
        for in_rel_info in all_incoming:
            if "source_id" in in_rel_info:
                neo4j_host.Crt_rel_by_id(
                    start_node_id=in_rel_info["source_id"],
                    end_node_id=same_name_node_id,   # 把最后一个节点作为保留的节点
                    relationship_type=in_rel_info["relationship_type"],
                    rel_attributes=in_rel_info["properties"]
                )
            else:
                print(in_rel_info)

        # 处理出边关系
        for out_rel_info in all_outgoing:
            if "target_id" in out_rel_info:
                neo4j_host.Crt_rel_by_id(
                    start_node_id=same_name_node_id,
                    end_node_id=out_rel_info["target_id"],
                    relationship_type=out_rel_info["relationship_type"],
                    rel_attributes=out_rel_info["properties"]
                )
            else:
                print(out_rel_info)

        # 删除多余的节点（保留最后一个）
        nodes_to_delete = node_id_list[:-1]
        for node_id in nodes_to_delete:
            neo4j_host.DeleteNode_by_id(node_id=node_id)

    except Exception as e:
        print(f"Error processing record {record}: {e}")

# 主程序
def main():
    while True:
        # 查询重复实体
        dup_entity = neo4j_host.execute_query(
            query='''
                MATCH (n:Entity)
                WHERE n.name IS NOT NULL
                WITH n.name AS name, COLLECT(n) AS nodes
                WHERE SIZE(nodes) > 1
                RETURN nodes
                LIMIT 500
            '''
        )

        if not dup_entity:
            print("全部完成")
            break

        # 使用多线程处理每个重复实体
        with ThreadPoolExecutor(max_workers=15) as executor:  # 设置最大线程数为10
            futures = [executor.submit(process_dup_entity, record) for record in dup_entity]

            # 显示进度条
            for future in tqdm(as_completed(futures), total=len(futures)):
                future.result()  # 确保捕获异常

if __name__ == "__main__":
    main()