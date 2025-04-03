from transformers import AutoTokenizer, AutoModelForTokenClassification, pipeline

def merge_ner_results(ner_results, sentence):
    "将ner得到的list[dict]转变为一个个单词，其中sentence是原来那句话"
    merged = []
    current_word = None
    current_entity = None
    current_scores = []
    start_pos = None
    end_pos = None

    for item in ner_results:
        # 解析实体标签
        entity_parts = item['entity'].split('-', 1)
        bio_tag = entity_parts[0]
        entity_type = entity_parts[1] if len(entity_parts) > 1 else None

        # 处理特殊字符（##开头的子词）
        clean_word = item['word'][2:] if item['word'].startswith('##') else item['word']

        if bio_tag == 'B':
            if current_word is not None:
                merged.append({
                    'word': current_word,
                    'entity': current_entity,
                    'start': start_pos,
                    'end': end_pos,
                    'min_score': min(current_scores)
                })
            current_word = clean_word
            current_entity = entity_type
            current_scores = [item['score']]
            start_pos = item['start']
            end_pos = item['end']
        elif (bio_tag == 'I') and (current_entity == entity_type) and (item['start']==end_pos):
            # 说明是不带空格的文字
            current_word += clean_word
            current_scores.append(item['score'])
            end_pos = item['end']
        elif (bio_tag == 'I') and (current_entity == entity_type) and (item['start']==end_pos+1 and sentence[end_pos]==" "):
            # 说明是带空格的文字
            current_word += " "
            current_word += clean_word
            current_scores.append(item['score'])
            end_pos = item['end']
        else:
            if current_word is not None:
                merged.append({
                    'word': current_word,
                    'entity': current_entity,
                    'start': start_pos,
                    'end': end_pos,
                    'min_score': min(current_scores)
                })
            current_word = clean_word if bio_tag == 'B' else None
            current_entity = entity_type if bio_tag == 'B' else None
            current_scores = [item['score']] if bio_tag == 'B' else []
            start_pos = item['start'] if bio_tag == 'B' else None
            end_pos = item['end'] if bio_tag == 'B' else None

    if current_word is not None:
        merged.append({
            'word': current_word,
            'entity': current_entity,
            'start': start_pos,
            'end': end_pos,
            'min_score': min(current_scores)
        })

    return merged

class NerHost:
    def __init__(self, local=True, model_name='Davlan-ner', model_path=r"E:\Coding\LLM"):
        if local:
            import os
            current_directory = os.getcwd()
            os.chdir(model_path)   # 先迁移到大模型文件夹
            tokenizer = AutoTokenizer.from_pretrained(model_name)
            ner_model = AutoModelForTokenClassification.from_pretrained(model_name)
            os.chdir(current_directory)  # 换回原来的目录下
        else:
            tokenizer = AutoTokenizer.from_pretrained("Davlan/bert-base-multilingual-cased-ner-hrl")
            ner_model = AutoModelForTokenClassification.from_pretrained("Davlan/bert-base-multilingual-cased-ner-hrl")
        self.ner_pipline=pipeline("ner", model=ner_model, tokenizer=tokenizer)
    def ner(self,sentence):
        "执行命名实体识别"
        raw_results=self.ner_pipline(sentence)
        merged_results=merge_ner_results(ner_results=self.ner_pipline(sentence), sentence=sentence)
        return raw_results, merged_results

