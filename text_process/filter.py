from numpy import isnan

def filter_missing_values(d):
    """
    过滤字典中的缺失值，包括空字符串、None、np.nan等。
    
    参数:
        d (dict): 输入的字典
        
    返回:
        dict: 只包含有效值的字典
    """
    return {
        k: v for k, v in d.items()
        if v is not None and not (isinstance(v, float) and isnan(v)) and v != ""
    }
    
def filter_dict_by_keys(d, keys, fill=False, fill_value=None):
    """
    根据给定的 key 列表提取字典中的键值对。
    
    参数:
        d (dict): 原始字典
        keys (list): 需要提取的键列表
        fill (bool): 如果为 True，缺失的键用 fill_value 填充；如果为 False，忽略缺失的键
        fill_value: 用于填充缺失键的默认值，默认为 None
        
    返回:
        dict: 包含指定键及其值的新字典
    """
    result = {}
    for key in keys:
        if key in d:
            result[key] = d[key]
        elif fill:
            result[key] = fill_value
    return result