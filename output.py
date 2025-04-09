"使用方法：在命令行运行，后面跟一个参数指定输出的文件名（不需要以graphml结尾，会自动添加后缀）"
import sys
import API.neo4j_SLPC
from networkx import write_graphml
neo4j_host=API.neo4j_SLPC.Neo4jClient(driver=API.neo4j_SLPC.local_driver)

import API.SQL_SPLC
sql_host=API.SQL_SPLC.generate_sql_host(database="splc")

from procedures.output_backup import Neo4jExporter, get_subnetwork, adapt_to_export_form, category_4_level
exporter=Neo4jExporter(neo4j_host=neo4j_host)
supply_net = exporter.export_supply_relations(check_rubbish=False)

supply_net=category_4_level(supply_net)
write_graphml(supply_net, sys.argv[1]+".graphml")