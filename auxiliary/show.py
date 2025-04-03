import matplotlib.pyplot as plt
import networkx as nx
plt.rcParams['font.sans-serif'] = ['SimHei']  # 使用黑体显示中文
plt.rcParams['axes.unicode_minus'] = False    # 正常显示负号
import random

def get_biggest_n(node_list, degree_dict,n):
    "返回一个节点列表当中度最大的n个节点"
    if len(node_list)<=n:
        return list(node_list)
    else:
        node_list_sorted=sorted(node_list,key=lambda x:degree_dict[x],reverse=True)
        return node_list_sorted[:n]


def draw_splc_graph(name, G, output_filename="", fig_size=[], tier_1_num=6, tier_2_num=15, random_scale=0.3):
    "基于networkx，绘制以name节点为中心的二级供应链网络图"
    degree_dict = dict(nx.degree(G))
    sub_nodes = set([name])
    
    # 收集客户侧节点
    tier_1_cus_list = []
    for i in nx.bfs_successors(G, name, 1):
        tier_1_cus_list += get_biggest_n(i[1], degree_dict, tier_1_num)
    tier_1_cus_list = get_biggest_n(tier_1_cus_list, degree_dict, tier_1_num)
    sub_nodes.update(tier_1_cus_list)
    
    tier_2_cus_set = set()
    for tier_1_cus in tier_1_cus_list:
        for j in nx.bfs_successors(G, tier_1_cus, 1):
            tier_2_cus_set.update(get_biggest_n(j[1], degree_dict, tier_2_num))
    tier_2_cus_list = get_biggest_n(tier_2_cus_set, degree_dict, tier_2_num)
    sub_nodes.update(tier_2_cus_list)
    
    # 收集供应商侧节点
    tier_1_sup_list = []
    for i in nx.bfs_predecessors(G, name, 1):
        tier_1_sup_list.append(i[0])
    tier_1_sup_list = get_biggest_n(tier_1_sup_list, degree_dict, tier_1_num)
    sub_nodes.update(tier_1_sup_list)
    
    tier_2_sup_set = set()
    for tier_1_sup in tier_1_sup_list:
        for j in nx.bfs_predecessors(G, tier_1_sup, 1):
            tier_2_sup_set.add(j[0])
    tier_2_sup_list = get_biggest_n(tier_2_sup_set, degree_dict, tier_2_num)
    sub_nodes.update(tier_2_sup_list)
    
    tier2_G = G.subgraph(sub_nodes)
    pos = {}
    
    # 预处理角色分类
    sup_nodes = set(tier_1_sup_list + tier_2_sup_list)
    cus_nodes = set(tier_1_cus_list + tier_2_cus_list)
    common_nodes = sup_nodes & cus_nodes  # 同时属于供应商和客户的节点
    
    # 分配供应商侧坐标（左侧）
    sup_all = list(sup_nodes - common_nodes)  # 仅供应商的节点
    num_sup = len(sup_all)
    for i, node in enumerate(sup_all):
        x = (-2 if node in tier_1_sup_list else -4)  + (random.random()-0.5)*random_scale
        y = i / max(num_sup - 1, 1)  # 归一化到[0,1]
        pos[node] = (x, y)
    
    # 分配客户侧坐标（右侧）
    cus_all = list(cus_nodes - common_nodes)  # 仅客户的节点
    num_cus = len(cus_all)
    for i, node in enumerate(cus_all):
        x = (2 if node in tier_1_cus_list else 4)  + (random.random()-0.5)*random_scale
        y = i / max(num_cus - 1, 1)  # 归一化到[0,1]
        pos[node] = (x, y)
    
    # 将核心节点插入 common_nodes 中间位置
    num_common = len(common_nodes)
    common_nodes = list(common_nodes)
    # 插入核心节点到中间位置
    if num_common == 0:
        # 如果没有共同节点，直接将核心节点放在中间
        common_nodes.append(name)
    else:
        # 计算插入位置（中间索引）
        insert_index = (num_common + 1) // 2
        common_nodes.insert(insert_index, name)

    # 更新 num_common
    num_common = len(common_nodes)

    # 调整节点位置，确保均匀分布
    for i, node in enumerate(common_nodes):
        pos[node] = (0  + (random.random()-0.5)*random_scale, i / max(num_common - 1, 1))  # 沿中线分布
    
    # 应用力导向布局微调（保持核心节点固定）
    # fixed = [name]
    # pos = nx.spring_layout(tier2_G, pos=pos, fixed=fixed, iterations=1, k=0.05)
    
    # 绘制图形
    country_dict = nx.get_node_attributes(tier2_G, "country")
    color_list = [
        "red" if "中国" in v and "台湾" not in v else "yellow"
        for v in country_dict.values()
    ]
    node_list = list(tier2_G.nodes())
    
    plt.figure(figsize=fig_size if fig_size else (12, 8))
    plt.title(f"{name} 供应链图谱")
    nx.draw_networkx(
        tier2_G, pos=pos, node_color=color_list,
        nodelist=node_list, arrows=True
    )
    
    if output_filename:
        plt.savefig(output_filename, bbox_inches="tight")
        plt.close()
    else:
        plt.show()
        plt.close()
    
    print("绘图完成，节点列表：", node_list)
def print_relation_data(data):
    """
    清晰打印关系数据

    Args:
        data: 一个包含多个字典的列表，每个字典代表一个实体关系。

    Returns:
        None
    """

    for relation in data:
        relation_type = relation['relation']['type'] if 'relation' in relation and 'type' in relation['relation'] else None
        print(f"关系: {relation_type if relation_type else '无关系类型'}")
        source_name = relation['source']['name'] if 'source' in relation and 'name' in relation['source'] else None
        print(f"  源实体:")
        print(f"    名称: {source_name if source_name else '无名称'}")
        source_type = relation['source']['type'] if 'source' in relation and 'type' in relation['source'] else None
        print(f"    类型: {source_type if source_type else '无类型'}")
        if 'source' in relation and 'info' in relation['source']:
            temp_info = relation['source'].get('info', {})
            if temp_info:
                for key, value in temp_info.items():
                    print(f"    {key}: {value}")

        target_name = relation['target']['name'] if 'target' in relation and 'name' in relation['target'] else None
        print(f"  目标实体:")
        print(f"    名称: {target_name if target_name else '无名称'}")
        target_type = relation['target']['type'] if 'target' in relation and 'type' in relation['target'] else None
        print(f"    类型: {target_type if target_type else '无类型'}")
        if 'target' in relation and 'info' in relation['target']:
            temp_info = relation['target'].get('info', {})
            if temp_info:
                for key, value in temp_info.items():
                    print(f"    {key}: {value}")

        reason_ori = relation['reason_ori'] if 'reason_ori' in relation else None
        print(f"  原因 (原始): {reason_ori if reason_ori else '无原始原因'}")
        reason_zh = relation['reason_zh'] if 'reason_zh' in relation else None
        print(f"  原因 (中文): {reason_zh if reason_zh else '无中文原因'}")
        print("-" * 20)