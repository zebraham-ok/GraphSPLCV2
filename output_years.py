"按年份分别导出网络"
# import API.neo4j_SPLC
# neo4j_host=API.neo4j_SPLC.Neo4jClient(driver=API.neo4j_SPLC.local_driver)
from Neo4jHost import get_reomte_driver
neo4j_host=get_reomte_driver()

from procedures.output_backup import *
import os

exporter=Neo4jExporter(neo4j_host)
G_dict=exporter.export_supply_relations_by_year(start_year=2013, end_year=2025)

output_path=r"result\YearOutput"
for year, G in G_dict.items():
    file_path=os.path.join(output_path, f"{year}.graphml")
    G=adapt_to_export_form(G)
    G=category_4_level(G)
    nx.write_graphml(G, file_path)
    print(f"Year {year} Network: {len(G.nodes())} nodes, {len(G.edges())} edges")