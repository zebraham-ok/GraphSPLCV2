from Neo4jHost import get_remote_driver
import matplotlib.pyplot as plt
import time
from datetime import datetime
from API.SQL_SPLC import generate_sql_host

# 初始化 Neo4j 驱动
neo4j_host = get_remote_driver()
sql_host = generate_sql_host(database="splc")

def get_verified_counts():
    """
    查询 r.verified 的计数。
    """
    query = """
    MATCH (:EntityObj)-[r:SupplyProductTo]->(:EntityObj)
    RETURN r.verified AS verified, COUNT(r) AS cnt_r
    """
    result = neo4j_host.execute_query(query)
    counts = {record["verified"]: record["cnt_r"] for record in result}
    return counts.get(None, 0)  # 返回 r.verified = NULL 的计数

def get_processed_ratio():
    """
    查询 processed_ratio
    """
    query = """
    MATCH (n:Article)
    WITH COUNT(n) AS totalArticles
    OPTIONAL MATCH (article:Article)<-[:SectionOf]-(section:Section)-[:Mention]->(entity:Entity)
    WITH totalArticles, COUNT(DISTINCT article) AS articlesWithSectionAndEntity
    RETURN 
        totalArticles, 
        articlesWithSectionAndEntity, 
        toFloat(articlesWithSectionAndEntity) / totalArticles AS processed_ratio
    """
    result = neo4j_host.execute_query(query)
    record = result[0]
    return record["processed_ratio"]

def get_des_done_number():
    query='''match (n:Company) where n.other_possible_industry_2 is not null
        return count(n) as des_done_num'''
    result = neo4j_host.execute_query(query)
    record = result[0]
    return record["des_done_num"]

def get_load_done_ratio():
    """
    查询 load_done_ratio。
    """
    query = """
        SELECT
            total_articles,
            CAST(useful_count / total_articles as DECIMAL(10,8)) AS `useful_done_ratio`,
            cast(load_count / total_articles as DECIMAL(10,8)) AS `load_done_ratio`
        FROM (
            SELECT
                COUNT(0) AS `total_articles`,
                COUNT(
                    CASE
                        WHEN (`crawler_main`.`useful` IS NOT NULL) THEN 1
                    END
                ) AS `useful_count`,
                COUNT(
                    CASE
                        WHEN (`crawler_main`.`load_time` IS NOT NULL) THEN 1
                    END
                ) AS `load_count`
            FROM `crawler_main`
        ) AS subquery;
    """
    result = sql_host._execute_query(query).fetchall()
    return result[0][-1]

def monitor_and_plot():
    """
    实时监控并绘制 processed_ratio、r.verified=NULL 的计数和 load_done_ratio 的曲线。
    """
    try:
        # 初始化绘图
        plt.ion()  # 开启交互模式
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(14, 8))  # 创建三个子图

        # 初始化数据
        times = []
        null_verified_counts = []
        processed_ratios = []
        load_done_ratios = []
        des_done_numbers = []

        while True:
            # 获取当前时间
            current_time = datetime.now()

            # 查询 r.verified = NULL 的计数
            null_verified_count = get_verified_counts()

            # 查询 processed_ratio
            processed_ratio = get_processed_ratio()

            # 查询 load_done_ratio
            load_done_ratio = get_load_done_ratio()
            
            des_done_number = get_des_done_number()

            # 更新数据
            times.append(current_time)
            null_verified_counts.append(null_verified_count)
            processed_ratios.append(processed_ratio)
            load_done_ratios.append(load_done_ratio)
            des_done_numbers.append(des_done_number)

            # 限制数据点数量（例如最近 60 秒的数据）
            # if len(times) > 60:
            #     times.pop(0)
            #     null_verified_counts.pop(0)
            #     processed_ratios.pop(0)
            #     load_done_ratios.pop(0)

            # 清空画布
            for ax in [ax1, ax2, ax3, ax4]:
                ax.clear()

            # 绘制第一个子图：Unverified Supply Count
            ax1.plot(times, null_verified_counts, 'b-', label='Unverified Supply Count')
            # ax1.set_title('Unverified Supply Count')
            ax1.set_ylabel('Count')
            ax1.legend(loc='upper left')

            # 绘制第二个子图：Article Processed Ratio
            ax2.plot(times, processed_ratios, 'r-', label='Article Processed Ratio')
            # ax2.set_title('Article Processed Ratio')
            ax2.set_ylabel('Ratio')
            ax2.legend(loc='upper left')

            # 绘制第三个子图：Load Done Ratio
            ax3.plot(times, load_done_ratios, 'g-', label='Load Done Ratio')
            # ax3.set_title('Load Done Ratio')
            ax3.set_xlabel('Time')
            ax3.set_ylabel('Ratio')
            ax3.legend(loc='upper left')
            
            # 绘制第四个子图：Load Done Ratio
            ax4.plot(times, des_done_numbers, 'y-', label='Des Done Number')
            # ax4.set_title('Load Done Ratio')
            ax4.set_xlabel('Time')
            ax4.set_ylabel('Number')
            ax4.legend(loc='upper left')

            # 刷新画布
            plt.draw()
            plt.gcf().canvas.flush_events()
            time.sleep(1)  # 每秒刷新一次

    except KeyboardInterrupt:
        print("\n监控已停止。")

if __name__ == "__main__":
    print("开始实时监控并绘制曲线...")
    monitor_and_plot()