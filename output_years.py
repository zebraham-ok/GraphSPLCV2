"按年份分别导出网络"
# import API.neo4j_SPLC
# neo4j_host=API.neo4j_SPLC.Neo4jClient(driver=API.neo4j_SPLC.local_driver)
from Neo4jHost import get_remote_driver
neo4j_host=get_remote_driver()

from procedures.output_relfection import *
import os

exporter=Neo4jExporter(neo4j_host)
output_path=r"result\YearOutput"

for year in range(2013, 2026):
    file_path=os.path.join(output_path, f"{year}.graphml")
    G=exporter.export_supply_relations_by_year(year=year)
    G=adapt_to_export_form(G)
    nx.write_graphml(G, file_path)
    print(f"Year {year} Network: {len(G.nodes())} nodes, {len(G.edges())} edges")