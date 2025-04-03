"这个函数会将云SQL服务器中的load_time清空（一般在neo4j清空的时候才会用这个操作"
import API.SQL_SPLC
db_client = API.SQL_SPLC.generate_sql_host(database="splc")
db_client.batch_update_column(table="crawler_main", column="load_time", value=None, pk_column="US_id", batch_size=500, where_clause=None, timeout=30, retries=1)