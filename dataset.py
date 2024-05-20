import json
import pandas as pd
import uuid
import time
import re


# 生成唯一标识符的函数
# def generate_unique_id():
#     return str(uuid.uuid4())

def preprocess(origin_csv_path: str) -> (int, float):
    # timer
    start_time = time.time()

    # load csv, delimiter='\t'
    df = pd.read_csv(origin_csv_path, delimiter="	", header=None)

    # 准备一个列表来存储结构化数据
    structured_data = []

    # 先给个自增唯一id
    tmp_unique_id = 0

    # debug
    # index, row = next(iter(df.iterrows()))

    # 解析数据集中的每个条目
    for index, row in df.iterrows():
        unique_id = tmp_unique_id
        tmp_unique_id += 1
        question_id = row.loc[0]
        # headline_str = row.loc[1]
        # 简单清洗 42534~42542存在的\n和多余的空格
        headline_str = re.sub(r'\s+$\n', '', row.loc[1])
        # question_str = row.loc[2]
        question_str = re.sub(r'\s+$\n', '', row.loc[2])
        gold_index = row.loc[3]
        class_id = row.loc[4]

        # 创建一个结构化字典
        structured_entry = {
            "Id": unique_id,
            "Question": question_str,
            # 简单处理 answer
            "Answer": "Yes" if gold_index == 1 else "No",
            "Headline": headline_str,
            "Options": [
                "No",
                "Yes"
            ],
            "Origin_question_id": question_id,
            "Gold_index": gold_index,
            "Class_id": class_id
        }

        # 添加到列表中
        structured_data.append(structured_entry)
    # 将结构化数据保存为JSON文件
    save_path = f"structured_{origin_csv_path.split('.')[0]}.json"
    with open(save_path, 'w') as json_file:
        json.dump(structured_data, json_file, indent=4)
    # 结束计时
    end_time = time.time()
    # 报告统计信息
    total_pairs = len(structured_data)
    time_taken = end_time - start_time

    print(f"{origin_csv_path}提取的问题-答案对总数: {total_pairs}")
    print(f"{origin_csv_path}转换过程所用时间: {time_taken} 秒")
    return total_pairs, time_taken


if __name__ == '__main__':
    total_pairs_train, time_taken_train = preprocess("train.csv")
    total_pairs_test, time_taken_test = preprocess("test.csv")
