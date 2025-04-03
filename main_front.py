import subprocess


# 定义要启动的脚本列表
# scripts = ["SQL2Neo4j.py", "NER_RE_Entity.py", "NER_RE_Product.py", "EntityDes.py","QwenEmbedding.py"]
scripts = ["NER_RE_Entity.py", "NER_RE_Product.py", "EntityDes.py","QwenEmbedding.py"]

# 启动每个脚本
for script in scripts:
    subprocess.Popen(["python", script])  # 使用 Popen 实现非阻塞启动