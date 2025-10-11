import pandas as pd
# import pymysql
from pymysql.err import MySQLError
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy import create_engine, text
from .ai_ask import read_secrets_from_csv

def escape_string(s):  # 在使用了参数化查询之后，其实可能已经不需要这个了
    """
    转义字符串中的单引号和双引号，使其在 MySQL 查询中安全使用。
    """
    if s is None:
        return None
    return s.replace("'", "''").replace('"', '""')

secret_file=r"API\secrets.csv"
secret_dict=read_secrets_from_csv(filename=secret_file)
def generate_sql_host(database, sql_host_name = secret_dict["sql_host"], 
                      sql_secret=secret_dict["sql_secret"],user=secret_dict["sql_user"],
                      sql_port=secret_dict["sql_port"]):
    "获取一个MySQL的接口"
    if isinstance(sql_port, int):
        sql_port=str(sql_port)
        
    sql_host=sql_host_name+":"+sql_port    
    return MySQLClient(host=sql_host, user=user, password=sql_secret, database=database)

class MySQLClient:
    def __init__(self, password:str, database:str, host="localhost", user="root"):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}/{database}')
        self.connection = self.engine.connect()

    def close(self):
        """关闭数据库连接"""
        if self.connection:
            self.connection.close()

    def _execute_query(self, query:str, params=None, timeout=30, retries=1, dict_mode=False, unpack=True):
        """执行SQL查询，支持超时终止和自动重试"""
        attempt = 0
        while attempt <= retries:
            try:
                with self.engine.begin() as conn:  # 使用上下文管理器确保事务安全
                    # 设置执行选项
                    conn.execution_options(timeout=timeout)
                    result_proxy = conn.execute(text(query), params)
                    if "SELECT" in query.upper():
                        if dict_mode and unpack:
                            return result_proxy.mappings().all()
                        elif unpack:
                            return result_proxy.fetchall()
                        else:
                            return result_proxy
                    else:
                        return None
            except (OperationalError, TimeoutError, ProgrammingError) as e:
                if attempt < retries:
                    print(f"Query execution failed, retrying... Attempt {attempt + 1}/{retries}")
                else:
                    print(f"Query execution failed after {retries} attempts: {e}")
                    raise
            finally:
                attempt += 1

    def get_all_tables(self):
        """查询数据库中所有的表名称"""
        query = "SHOW TABLES"
        result_proxy = self._execute_query(query)
        tables = [row[0] for row in result_proxy]
        return tables

    def get_table_fields(self, table_name):
        """查询某一个表中所有field的名称"""
        query = f"DESCRIBE {table_name}"
        result_proxy = self._execute_query(query)
        fields = [row[0] for row in result_proxy]
        return fields

    def delete_table(self, table_name):
        """删除指定名称的表"""
        try:
            query = f"DROP TABLE IF EXISTS {table_name}"
            self._execute_query(query)
            print(f"Table '{table_name}' has been deleted successfully.")
        except MySQLError as e:
            print(f"Error deleting table '{table_name}': {e}")

    def df_to_table(self, dataframe, table_name, if_exists='fail', index=False):
        """
        将 Pandas DataFrame 导入为 MySQL 表格。
        
        :param dataframe: 要导入的数据帧
        :param table_name: 目标表名
        :param if_exists: 如果表已存在，'fail' 抛出错误，'replace' 替换表，'append' 追加数据
        :param index: 是否将 DataFrame 的索引作为一列写入
        """
        try:
            dataframe.to_sql(name=table_name, con=self.engine, if_exists=if_exists, index=index)
        except Exception as e:
            print(f"Error: {e}")
            raise

    def table_to_df(self, table_name):
        """
        从 MySQL 表格导出为 Pandas DataFrame。
        
        :param table_name: 源表名
        :return: 包含表数据的 DataFrame
        """
        query = f"SELECT * FROM {table_name}"
        result_proxy = self._execute_query(query)
        return pd.DataFrame(result_proxy, columns=result_proxy.keys())
    
    def create_table_from_dict(self, table_name, dict_data):
        """
        如果表已经存在，会给出提示；如果表不存在，创建一个包含 key 和 value 两列的新表，然后插入数据。
        
        :param table_name: 表名
        :param dict_data: 要插入的字典数据
        """
        query = f"SHOW TABLES LIKE '{table_name}'"
        result = self._execute_query(query)
        
        if result:
            existing_table_fields = self.get_table_fields(table_name)
            print(f"已经存在一个具有field: {existing_table_fields}的表，请决定是否要删除它")
            return
        else:
            create_table_query = f"CREATE TABLE {table_name} (id INT AUTO_INCREMENT PRIMARY KEY, dict_key VARCHAR(255) COLLATE utf8_bin UNIQUE, dict_value VARCHAR(255))"
            self._execute_query(create_table_query)
            for key, value in dict_data.items():
                insert_query = f"INSERT INTO {table_name} (dict_key, dict_value) VALUES ('{escape_string(key)}', '{escape_string(value)}') ON DUPLICATE KEY UPDATE dict_value='{value}'"
                self._execute_query(insert_query)

    # 能够实现类似于将SQL当成一个dict的感觉
    def query_column_as_dict(self, table_name, key_column="dict_key", value_column="dict_value"):
        """
        从表中查询指定的两列，并以字典形式返回结果。
        
        :param table_name: 表名
        :param key_column: 键列名
        :param value_column: 值列名
        :return: 字典，键为 key_column 的值，值为 value_column 的值
        """
        query = f"SELECT {key_column}, {value_column} FROM {table_name}"
        result = self._execute_query(query)
        return {row[0]: row[1] for row in result}

    def create_or_insert_list(self, table_name, list_data):
        """
        如果表已经存在，将列表数据插入到表中；如果表不存在，创建一个包含 key 和 value 两列的新表，然后插入数据。
        
        :param table_name: 表名
        :param list_data: 要插入的列表数据
        """
        query = f"SHOW TABLES LIKE '{table_name}'"
        result = self._execute_query(query)
        
        if result:
            existing_table_fields = self.get_table_fields(table_name)
            if existing_table_fields != ["item"]:
                print(f"已经存在一个具有field: {existing_table_fields}的表，请决定是否要删除它")
                return
        else:
            create_table_query = f"CREATE TABLE {table_name} (id INT AUTO_INCREMENT PRIMARY KEY, item VARCHAR(255) UNIQUE)"
            self._execute_query(create_table_query)
            
        for item in list_data:
            # 使用参数化查询来避免SQL注入
            insert_query = f"INSERT IGNORE INTO {table_name} (item) VALUES (:item)"
            self._execute_query(insert_query, {"item": item})
            
    def insert_items_to_list_like_table(self, table_name, data,column_name="item",ignore="IGNORE"):  # 使用这个函数的时候需要提前就知道这个列表中的field是什么
        """
        向一个只有一个item列的列表中插入一行或多行。
        
        :param table_name: 目标表名
        :param data: 要插入的数据，可以是单个字典或字典列表，其中键为列名，值为对应的数据
        """
        if not isinstance(data, list):
            data = [data]

        for value in data:
            insert_query = f"INSERT {ignore} INTO {table_name} ({column_name}) VALUES (:item)"
            self._execute_query(insert_query,params={"item":value})

    def insert_row(self, table_name: str, data: dict, ignore="IGNORE"):
        """
        插入一行数据，如果存在冲突则更新该行数据
        
        :param table_name: 目标表名
        :param data: 要插入或更新的数据，键为列名，值为对应的数据
        :param ignore: 是否忽略重复键错误，默认是 IGNORE，如果要update，则设置为""
        """
        column_names = ", ".join(data.keys())
        place_holders = ", ".join([":" + str(key) for key in data.keys()])
        set_clause = ", ".join([f"{key} = :{key}" for key in data.keys()])
        insert_query = (
            f"INSERT {ignore} INTO {table_name} ({column_names}) VALUES ({place_holders}) "
            f"ON DUPLICATE KEY UPDATE {set_clause}"
        )
        self._execute_query(insert_query, params=data, unpack=False)

    def insert_multiple_rows(self, table_name, list_data, column_names:list, ignore="IGNORE"):
        """
        向特定表中批量插入多行数据。

        :param table_name: 目标表名
        :param list_data: 要插入的数据列表，每个元素为一个包含列名和对应值的字典
        :param ignore: 是否忽略重复的记录，默认为'IGNORE'表示忽略，否则为空字符串
        """
        if not isinstance(list_data, list) or len(list_data) == 0:
            return
        if len(column_names)!=len(list_data[0]):
            print("length of column does not match length of data")
            return
        
        # 获取列名
        columns = ', '.join(column_names)
        # 创建占位符列表
        placeholders = ', '.join([":"+i for i in column_names])

        # 插入语句
        sql_insert_query = f"INSERT {ignore} INTO {table_name} ({columns}) VALUES ({placeholders})"
        # print(sql_insert_query)
        
        for row in list_data:
            params={column_names[i]:row[i] for i in range(len(column_names))}
            self._execute_query(query=sql_insert_query,params=params, unpack=False)
            
    def check_item_exists(self, table_name, item, column_name="item", strict_equal=True):
        """
        检查表中的某个列是否包含特定的字符串元素。
        
        :param table_name: 表名
        :param column_name: 列名
        :param item: 要检查的字符串元素
        :return: 如果存在返回 True，否则返回 False
        :strict_equal: 如果是False的话，只要包含就算
        """
        
        if strict_equal:
            query = f"SELECT 1 FROM {table_name} WHERE {column_name} = :item"
            result = self._execute_query(query, {"item": item})
        else:
            query = f"SELECT 1 FROM {table_name} WHERE {column_name} like :item"
            result = self._execute_query(query, {"item": f"%{item}%"})

        if result:
            return True
        else:
            return False
    
    def reflect_of_column_value(self, table_name, key, key_column="dict_key",value_column="dict_value",if_missing=None):
        """
        将sql当做一个字典使用，如果没有那个键，则返回None
        如果if_missing=key_back，就会在没有找到匹配的对象时将key直接作为返回值返回
        """
        query = f"SELECT {value_column} FROM {table_name} WHERE {key_column} = (:key_content)"
        try:
            results = self._execute_query(query,params={"key_content":key})
            if results:
                return results[0]
            elif if_missing=="key_back":
                return key
        except OperationalError as e:
            # 检查错误码是否为 1267
            if e.orig.args[0] == 1267:
                print(f"捕获到字符集不匹配错误： encountered in reflect_of_column_value({table_name},{key},{key_column},{value_column}), {e}")
                return "Error 1267"
            else:
                print(f"Error encountered in reflect_of_column_value({table_name},{key},{key_column},{value_column}), {e}")
                return key
        except Exception as e:
            print(f"Error encountered in reflect_of_column_value({table_name},{key},{key_column},{value_column}), {e}")
            return key
    def reverse_reflect_of_value(self, table_name, value, key_column="dict_key", value_column="dict_value"):
        """
        将sql当做一个字典使用，如果找到匹配的值，则返回所有等于这个值的键的列表；
        如果没有找到任何匹配项，则返回空列表。
        """
        query = f"SELECT {key_column} FROM {table_name} WHERE {value_column} = (:value)"
        try:
            results = self._execute_query(query, params={"value": value})
            keys = [result[0] for result in results]        # 提取每个结果的第一列（即key_column的值）
            
            return keys
        except Exception as e:
            print(f"Error encountered with reverse_reflect_of_value({table_name}, {value}, {key_column}, {value_column}), error info: {e}")
            return []
    
    def batch_update_column(self, table, column, value, pk_column, batch_size=1000, where_clause=None, timeout=30, retries=1):
        """
        分批次更新表中指定列的值（MySQL专用改进版），增加进度显示功能
        
        参数说明：
        table - 表名
        column - 要修改的列名
        value - 要设置的新值（支持None表示设置NULL）
        pk_column - 主键列名（用于排序分页）
        batch_size - 每批次更新行数
        where_clause - 筛选条件（可包含命名参数占位符）
        """
        max_pk = None
        total_updated = 0
        params_dict = {}

        # 查询总行数
        conditions_for_count = []
        if where_clause:
            conditions_for_count.append(where_clause)
        count_sql = f"""
            SELECT COUNT(*)
            FROM {table}
            {f'WHERE {" AND ".join(conditions_for_count)}' if conditions_for_count else ""}
        """
        total_rows_result = self._execute_query(count_sql, params_dict or None, timeout, retries)
        total_rows = total_rows_result.scalar()
        
        if total_rows == 0:
            print("没有需要更新的数据")
            return 0

        print(f"总共需要更新 {total_rows} 行数据")

        while True:
            # 构建条件表达式
            conditions = []
            if where_clause:
                conditions.append(where_clause)
            if max_pk is not None:
                conditions.append(f"{pk_column} > :current_max")
                params_dict["current_max"] = max_pk

            # 构建SET子句（区分NULL和普通值）
            set_expression = f"{column} = NULL" if value is None else f"{column} = :new_value"
            if value is not None:
                params_dict["new_value"] = value

            # 组装完整UPDATE语句
            update_sql = f"""
                UPDATE {table}
                SET {set_expression}
                {f'WHERE {" AND ".join(conditions)}' if conditions else ""}
                ORDER BY {pk_column}
                LIMIT {batch_size}
            """
            
            if total_updated == 0:
                print("修改执行中: ", update_sql)
                
            # 执行批量更新
            result = self._execute_query(update_sql, params_dict or None, timeout, retries)
            affected_rows = result.rowcount
            total_updated += affected_rows

            # 计算并显示进度
            progress = (total_updated / total_rows) * 100
            print(f"进度: {total_updated}/{total_rows} ({progress:.2f}%)")

            if affected_rows == 0:
                break

            # 获取当前批次最大主键（使用相同参数条件）
            select_max_sql = f"""
                SELECT MAX({pk_column})
                FROM (
                    SELECT {pk_column}
                    FROM {table}
                    {f'WHERE {" AND ".join(conditions)}' if conditions else ""}
                    ORDER BY {pk_column}
                    LIMIT {batch_size}
                ) AS tmp
            """
            max_result = self._execute_query(select_max_sql, params_dict, timeout, retries)
            current_max = max_result.scalar()

            if current_max is None:
                break
                
            max_pk = current_max
            params_dict["current_max"] = max_pk  # 更新循环参数

        print(f"批量更新完成，共更新{total_updated}行")
        return total_updated
    
    def copy_table_between_databases(self, src_table: str, dest_client, batch_size=1000):
        """
        从源数据库的表复制数据到目标数据库的同名表中。
        
        参数:
            src_table (str): 源数据库中的表名。
            dest_client (MySQLClient): 目标数据库的MySQLClient实例。
            batch_size (int): 每批次处理的数据行数，默认为1000。
        """
        # 查询源表的总记录数
        count_query = f"SELECT COUNT(*) FROM {src_table}"
        total_rows = self._execute_query(count_query).scalar()
        print(f"Total rows to be copied: {total_rows}")
        
        # 开始分批次复制
        offset = 0
        while offset < total_rows:
            query = f"SELECT * FROM {src_table} LIMIT {batch_size} OFFSET {offset}"
            data_to_copy = pd.read_sql(query, con=self.connection)
            
            # 使用pandas的to_sql方法将数据插入目标数据库
            data_to_copy.to_sql(src_table, con=dest_client.connection, if_exists='append', index=False)
            
            offset += batch_size
            print(f"Copied rows {offset} of {total_rows}")

            
if __name__=="__main__":
    # example_data = [
    #     {'id': 1, 'name': 'Alice', 'age': 30},
    #     {'id': 2, 'name': 'Bob', 'age': 25},
    #     {'id': 3, 'name': 'Charlie', 'age': 35}
    # ]

    # example_dict_data = {
    #     'key1': 'value1',
    #     'key2': 'value2',
    #     'key3': 'value3'
    # }

    # # 创建 MySQLClient 实例
    # db_client = MySQLClient(host='localhost', user='root', password='mysql', database='test')
    db_client = generate_sql_host(database="splc")
    db_client.batch_update_column(table="crawler_main", column="load_time", value=None, pk_column="US_id", batch_size=500, where_clause=None, timeout=30, retries=1)
    # print("Testing get_all_tables:")
    # tables = db_client.get_all_tables()
    # print(tables)
    
    # db_client.create_or_insert_list(table_name="url", list_data=["www","http"])

    # # 测试创建或插入字典
    # print("\nTesting create_or_insert_dict:")
    # db_client.create_table_from_dict('example_dict_table', example_dict_data)
    # print("Table created or data inserted.")

    # # 测试查询表中的两列并以字典形式返回
    # print("\nTesting query_column_as_dict:")
    # dict_result = db_client.query_column_as_dict('example_dict_table', 'dict_key', 'dict_value')
    # print(dict_result)

    # # 测试将 DataFrame 导入为 MySQL 表格
    # print("\nTesting df_to_table:")
    # df = pd.DataFrame(example_data)
    # db_client.df_to_table(df, 'example_table', if_exists='replace', index=False)
    # print("DataFrame imported to MySQL table.")

    # # 测试从 MySQL 表格导出为 Pandas DataFrame
    # print("\nTesting table_to_df:")
    # result_df = db_client.table_to_df('example_table')
    # print(result_df)

    # # 测试插入新行
    # # print("\nTesting insert_rows:")
    # # new_data = {'id': 4, 'name': 'David', 'age': 40}
    # # db_client.insert_rows('example_table', new_data)
    # # print("New row inserted.")

    # # 测试根据列值获取行
    # print("\nTesting reflect_of_column_value:")
    # result_list = db_client.reflect_of_column_value('example_table', column_name="name", key="Alice")
    # print(result_list)

    # # 测试获取表字段
    # print("\nTesting get_table_fields:")
    # fields = db_client.get_table_fields('example_table')
    # print(fields)

    # # 测试删除表
    # print("\nTesting delete_table:")
    # db_client.delete_table('example_table')
    # print("Table deleted.")