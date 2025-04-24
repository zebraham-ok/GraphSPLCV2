import matplotlib.pyplot as plt
import networkx as nx
plt.rcParams['font.sans-serif'] = ['SimHei']  # 使用黑体显示中文
plt.rcParams['axes.unicode_minus'] = False    # 正常显示负号
import random

from matplotlib.cm import get_cmap
from matplotlib.colors import Normalize
# from matplotlib.cm import ScalarMappable
from matplotlib.lines import Line2D

from collections import deque

# expand_to_double_tree先获取一个节点的双头树，visualize_supply_chain再对这个双头树进行可视化

def expand_to_double_tree(G, core_node, 
                         max_supplier_depth=None,
                         max_customer_depth=None):
    """
    参数说明：
    - max_supplier_depth: 供应商侧最大追溯层数（正整数）
    - max_customer_depth: 客户侧最大追溯层数（正整数）
    """
    new_G = nx.DiGraph()
    if core_node not in G:
        raise ValueError("核心节点不在图中")
    
    new_G.add_node(core_node, **G.nodes[core_node])
    
    def process_side(is_supplier, max_depth):
        direction = 'predecessors' if is_supplier else 'successors'
        suffix = 'S' if is_supplier else 'C'
        # 如果是客户，则会调换顺序
        edge_direction = (lambda u, v: (u, v)) if is_supplier else (lambda u, v: (v, u))
        
        queue = deque([(core_node, core_node, 0, {core_node})])
        level_counter = {}
        
        while queue:
            orig, copy_node, level, path = queue.popleft()
            
            # 达到最大深度时停止
            if max_depth is not None and level >= max_depth:
                continue
                
            for neighbor in getattr(G, direction)(orig):
                if neighbor in path:
                    continue
                
                # 计算新层级
                new_level = level + 1
                
                # 层级控制（在加入队列前检查）
                if max_depth is not None and new_level > max_depth:
                    continue
                
                # 生成节点名称
                key = (neighbor, new_level)
                level_counter[key] = level_counter.get(key, 0) + 1
                count = level_counter[key]
                
                node_suffix = f"{new_level}{suffix}" if count == 1 else f"{new_level}{suffix}-{count-1}"
                new_node = f"{neighbor}-{node_suffix}"
                
                # 添加节点和边
                if new_node not in new_G:
                    new_G.add_node(new_node, **G.nodes[neighbor])
                
                orig_edge = edge_direction(neighbor, orig)
                edge_data = G.get_edge_data(*orig_edge)
                new_edge = edge_direction(new_node, copy_node)
                
                if edge_data:
                    new_G.add_edge(*new_edge, **edge_data)
                else:
                    new_G.add_edge(*new_edge)
                
                # 更新路径和层级
                new_path = path.copy()
                new_path.add(neighbor)
                queue.append((neighbor, new_node, new_level, new_path))
    
    # 处理供应商侧（带深度限制）
    process_side(is_supplier=True, max_depth=max_supplier_depth)
    
    # 处理客户侧（带深度限制）
    process_side(is_supplier=False, max_depth=max_customer_depth)
    
    return new_G

import networkx as nx
import matplotlib.pyplot as plt
from matplotlib.cm import get_cmap
from matplotlib.colors import Normalize
from matplotlib.lines import Line2D
# from matplotlib.cm import ScalarMappable

def visualize_supply_chain(G, core_node,
                          node_color_attr=None,
                          edge_color_attr=None,
                          max_node_attr=5,
                          max_edge_attr=5,
                          forbidden_names=[],
                          figsize=(12, 8),
                          node_size=3000,
                          font_size=10,
                          legend_cols=3,
                          legend_font_scale=0.8,
                          legend_figsize=(4, 6),
                          dpi=300):
    """
    改进后的供应链可视化函数：
    - 使用重心法对 2S/2C 层节点排序，联动上下游节点更紧凑。
    - 节点/边颜色映射支持数值和分类型属性。
    - 将主图和图例分开到两个 Figure ，方便手动布局。
    """
    # ============= 数据过滤 =============
    filtered = nx.DiGraph()
    for n, data in G.nodes(data=True):
        if any(fn in n for fn in forbidden_names):
            continue
        if n == core_node or any(suf in n for suf in ['-1S', '-2S', '-1C', '-2C']):
            filtered.add_node(n, **data)
    for u, v, data in G.edges(data=True):
        if u in filtered and v in filtered:
            filtered.add_edge(u, v, **data)

    # ============= 分层分组 =============
    layers = ['2S', '1S', 'core', '1C', '2C']
    layer_groups = {L: {} for L in layers}
    for n in filtered.nodes():
        if n == core_node:
            layer = 'core'; name = core_node
        else:
            base = n.split('-')[0]
            if '-2S' in n: layer = '2S'
            elif '-1S' in n: layer = '1S'
            elif '-1C' in n: layer = '1C'
            elif '-2C' in n: layer = '2C'
            else: continue
            name = base
        layer_groups[layer].setdefault(name, []).append(n)

    # ============= 位置计算 =============
    pos = {}
    pos[core_node] = (2, 0)
    _assign_layer_positions('1S', x=1, layer_groups=layer_groups, pos=pos)
    _assign_layer_positions_barycenter('2S', x=0, layer_groups=layer_groups,
                                       pos=pos, filtered=filtered,
                                       direction='succ')
    _assign_layer_positions('1C', x=3, layer_groups=layer_groups, pos=pos)
    _assign_layer_positions_barycenter('2C', x=4, layer_groups=layer_groups,
                                       pos=pos, filtered=filtered,
                                       direction='pred')

    # ============= 颜色映射 =============
    node_legend = []
    if node_color_attr:
        attrs = [filtered.nodes[n].get(node_color_attr) for n in filtered]
        is_num = all(isinstance(a,(int,float)) for a in attrs if a is not None)
        if is_num:
            val = [a for a in attrs if a is not None]
            cmap = get_cmap('viridis')
            norm = Normalize(vmin=min(val) if val else 0, vmax=max(val) if val else 1)
            node_colors = [cmap(norm(a)) if a is not None else '#888888' for a in attrs]
        else:
            from collections import Counter
            cnt = Counter(a for a in attrs if a is not None)
            top = [a for a,_ in cnt.most_common(max_node_attr)]
            cmap = get_cmap('tab10')
            mapping = {cat:cmap(i%10) for i,cat in enumerate(top)}
            node_colors = [mapping.get(a,'#888888') for a in attrs]
            node_legend = [Line2D([0],[0], marker='o', color='w', markerfacecolor=mapping[cat],
                                  markersize=10, label=str(cat)) for cat in top]
    else:
        node_colors = '#1f78b4'

    edge_legend = []
    if edge_color_attr:
        eattrs = [filtered.edges[e].get(edge_color_attr) for e in filtered.edges()]
        is_num_e = all(isinstance(a,(int,float)) for a in eattrs if a is not None)
        if is_num_e:
            val = [a for a in eattrs if a is not None]
            cmap_e = plt.cm.plasma
            norm_e = Normalize(vmin=min(val) if val else 0, vmax=max(val) if val else 1)
            edge_colors = [cmap_e(norm_e(a)) if a is not None else '#808080' for a in eattrs]
        else:
            from collections import Counter
            cnt = Counter(a for a in eattrs if a is not None)
            top = [a for a,_ in cnt.most_common(max_edge_attr)]
            cmap_e = plt.cm.tab20
            mapping_e = {cat:cmap_e(i%20) for i,cat in enumerate(top)}
            edge_colors = [mapping_e.get(a,'#808080') for a in eattrs]
            edge_legend = [Line2D([0],[0], color=mapping_e[cat], lw=2, label=str(cat)) for cat in top]
    else:
        edge_colors = '#808080'

    legends = node_legend + edge_legend

    # ============= 主图: 仅绘制网络 =============
    fig1, ax1 = plt.subplots(figsize=figsize, dpi=dpi)
    nx.draw_networkx_nodes(filtered, pos, node_size=node_size, node_color=node_colors, alpha=0.9, ax=ax1)
    nx.draw_networkx_edges(filtered, pos, edge_color=edge_colors, arrowsize=20,
                           connectionstyle="arc3,rad=0.1", ax=ax1)
    labels = {n:n.split('-')[0] for n in filtered}
    nx.draw_networkx_labels(filtered, pos, labels, font_size=font_size, ax=ax1)
    plt.sca(ax1)
    plt.xlim(-0.5,4.5)
    plt.axis('off')
    # 添加层级文字
    y_vals = [pos[n][1] for n in pos]
    bottom = min(y_vals) - 0.05
    for x, txt in enumerate(["二级供应商","一级供应商","核心企业","一级客户","二级客户"]):
        plt.text(x, bottom, txt, ha='center', fontsize=font_size)

    # ============= 图例图: 单独 Figure =============
    fig2, ax2 = plt.subplots(figsize=legend_figsize, dpi=dpi)
    ax2.axis('off')
    if legends:
        ax2.legend(handles=legends, loc='center', ncol=legend_cols, frameon=False,
                   fontsize=font_size*legend_font_scale, title="图例说明")

    plt.show()


def _assign_layer_positions(layer, x, layer_groups, pos):
    items = sorted(layer_groups.get(layer,{}).keys())
    if not items: return
    step = 1.0/(len(items)+1)
    for i,name in enumerate(items):
        y=(i+1)*step - 0.5
        for node in layer_groups[layer][name]: pos[node]=(x,y)


def _assign_layer_positions_barycenter(layer, x, layer_groups, pos,
                                       filtered, direction='succ'):
    names=list(layer_groups.get(layer,{}).keys())
    if not names: return
    bary={}
    for name in names:
        ys=[]
        for node in layer_groups[layer][name]:
            nbrs=(filtered.successors(node) if direction=='succ' else filtered.predecessors(node))
            for nbr in nbrs:
                if nbr in pos: ys.append(pos[nbr][1])
        bary[name]=sum(ys)/len(ys) if ys else 0
    ordered=sorted(names, key=lambda n: bary[n])
    step=1.0/(len(ordered)+1)
    for i,name in enumerate(ordered):
        y=(i+1)*step - 0.5
        for node in layer_groups[layer][name]: pos[node]=(x,y)

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