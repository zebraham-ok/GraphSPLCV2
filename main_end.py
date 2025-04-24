
from Neo4jHost import get_reomte_driver
from main.SupplyVerify import supply_verify_main
from main.ProductCateRec import product_cate_rec_main

from concurrent.futures import ThreadPoolExecutor

# 定义主函数
def main_end():
    # 使用线程池并行运行两个函数
    with ThreadPoolExecutor(max_workers=2) as executor:
        future1 = executor.submit(supply_verify_main, get_reomte_driver(), 15)
        # future2 = executor.submit(product_cate_rec_main, get_reomte_driver(), 10)
        
        # 等待两个任务完成（可选）
        future1.result()
        # future2.result()

if __name__ == "__main__":
    main_end()