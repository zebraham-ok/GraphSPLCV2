from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import API.SQL_SPLC
sql_host = API.SQL_SPLC.generate_sql_host(database="splc")

def check_rubbish(name, print_rubbish=True):
    """检查单个节点是否是垃圾节点（线程安全版本）"""
    try:
        judge=sql_host.check_item_exists(table_name="rubbish_nodes", item=name)
        if print_rubbish and judge:
            print(f"{name} is labeled as rubbish")
        return judge
    except Exception as e:
        print(f"Error checking {name}: {str(e)}")
        return False  # 默认标记为非垃圾节点

def process_batch(neo4j_host, records):
    """多线程处理批量记录"""
    results = []
    
    # 为每个线程创建独立的SQL连接（确保线程安全）
    local_sql_host = API.SQL_SPLC.generate_sql_host(database="splc")
    
    def _task(record):
        e_id = record["id"]
        name = record["name"]
        judge = local_sql_host.check_item_exists(table_name="rubbish_nodes", item=name)
        return (e_id, judge)
    
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(_task, record) for record in records]
        for future in tqdm(as_completed(futures), total=len(records), desc="Processing"):
            try:
                results.append(future.result())
            except Exception as e:
                print(f"Thread error: {str(e)}")
    
    return results

if __name__ == "__main__":
    import API.neo4j_SLPC
    neo4j_host = API.neo4j_SLPC.Neo4jClient()
    
    skip = 0
    limit = 1000  # 每批处理量
    batch_size = 4  # 并行批次数
    
    with tqdm(desc="Total Progress") as pbar:
        while True:
            # 批量获取数据
            records = neo4j_host.execute_query(
                "MATCH (n:EntityObj) WHERE n.rubbish IS NULL "
                "RETURN elementid(n) AS id, n.name AS name "
                "SKIP $skip LIMIT $limit",
                parameters={"skip": skip, "limit": limit * batch_size}
            )
            
            if not records:
                break
                
            # 分割为多个批次并行处理
            chunks = [records[i:i+limit] for i in range(0, len(records), limit)]
            
            with ThreadPoolExecutor(max_workers=batch_size) as batch_executor:
                batch_futures = [batch_executor.submit(process_batch, neo4j_host, chunk) for chunk in chunks]
                
                all_results = []
                for future in as_completed(batch_futures):
                    try:
                        all_results.extend(future.result())
                    except Exception as e:
                        print(f"Batch error: {str(e)}")
                        
            # 批量更新Neo4j（减少数据库交互次数）
            update_query = (
                "UNWIND $updates AS update "
                "MATCH (n) WHERE elementid(n) = update.id "
                "SET n.rubbish = update.judge"
            )
            neo4j_host.execute_query(
                update_query,
                parameters={"updates": [{"id": eid, "judge": judge} for eid, judge in all_results]}
            )
            
            skip += len(records)
            pbar.update(len(records))