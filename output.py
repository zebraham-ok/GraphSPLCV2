"使用方法：在命令行运行，后面跟一个参数指定输出的文件名（不需要以graphml结尾，会自动添加后缀）"
import sys
import API.neo4j_SLPC
from networkx import write_graphml
neo4j_host=API.neo4j_SLPC.Neo4jClient(driver=API.neo4j_SLPC.local_driver)

import API.SQL_SPLC
sql_host=API.SQL_SPLC.generate_sql_host(database="splc")

from procedures.output_backup import Neo4jExporter, get_subnetwork
exporter=Neo4jExporter(neo4j_host=neo4j_host)
supply_net = exporter.export_supply_relations(check_rubbish=False)


# 将不同级别的连边分别导出

for edge in supply_net.edges():
    edge_info=supply_net.edges[edge]
    cate_list=edge_info["product_category"].split("-")
    
    edge_info["product_category_1"]=cate_list[0] if cate_list else ""
    edge_info["product_category_2"]="-".join(cate_list[:2]) if len(cate_list)>=2 else edge_info["product_category_1"]
    edge_info["product_category_3"]="-".join(cate_list[:3]) if len(cate_list)>=3 else edge_info["product_category_2"]
    edge_info["product_category_4"]=edge_info["product_category"]
    del edge_info["product_category"]
    
supply_net=exporter.adapt_to_export_form(supply_net)
write_graphml(supply_net, sys.argv[1]+".graphml")