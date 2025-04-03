import subprocess


# 定义要启动的脚本列表
scripts = ["ProductCateRec.py", "SupplyVerify.py"]

# 启动每个脚本
for script in scripts:
    subprocess.Popen(["python", script])  # 使用 Popen 实现非阻塞启动