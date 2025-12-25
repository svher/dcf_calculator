# %% [code]
##建立sparkSession
# coding=utf-8
from pyspark.sql import SparkSession
import pandas as pd
import numpy as np
from tqdm import tqdm
import os
from datetime import datetime, timedelta
import time
import networkx as nx
import math
import random
import json
import re
spark = SparkSession.builder.enableHiveSupport().getOrCreate()
sc = spark.sparkContext
today = str(${date})
sql = spark.sql
today_placeholder = '$[date]'
print("today: {}".format(today))

# %% [code]
import functools
import concurrent.futures
def split_chunks(n, lst):
    return [lst[i:i+n] for i in range(0, len(lst), n)]

# %% [code]
group_data_sql = """
SELECT  group_type,
        group_id,
        group_size,
        shop_id,
        shop_type_code,
        operate_status_code,
        stop_status_code,
        shop_open_time,
        is_seed,
        seed_penalize_info,
        first_seed_penalize_time,
        product_cnt,
        p60d_update_product_cnt,
        latest_product_update_time,
        p60d_create_product_cnt,
        latest_product_create_time,
        p60d_pay_order_cnt,
        p30d_pay_order_cnt,
        strong_group_id,
        first_seed_grouprejectpenalize_time
FROM    ecom_govern_community.groupaudit_shop_recallstrategy_v1_filter_groupshopinfo
WHERE   date='${date}'
AND     risk_type='xjxc'
"""
group_data=sql(group_data_sql).toPandas()
print(len(group_data))
group_data

# %% [code]
shop_info_dict = {}
for idx in tqdm(range(len(group_data))):
    shop_id = group_data['shop_id'].iloc[idx]
    shop_type_code = group_data['shop_type_code'].iloc[idx]
    operate_status_code = group_data['operate_status_code'].iloc[idx]
    stop_status_code = group_data['stop_status_code'].iloc[idx]
    shop_open_time = group_data['shop_open_time'].iloc[idx]
    is_seed = group_data['is_seed'].iloc[idx]
    seed_penalize_info = group_data['seed_penalize_info'].iloc[idx]
    first_seed_penalize_time = group_data['first_seed_penalize_time'].iloc[idx]
    p60d_update_product_cnt = group_data['p60d_update_product_cnt'].iloc[idx]
    strong_group_id = group_data['strong_group_id'].iloc[idx]
    first_seed_grouprejectpenalize_time = group_data['first_seed_grouprejectpenalize_time'].iloc[idx]
    shop_info_dict[shop_id] = {
        'shop_type_code': shop_type_code,
        'operate_status_code': operate_status_code,
        'stop_status_code': stop_status_code,
        'shop_open_time': shop_open_time,
        'is_seed': is_seed,
        'seed_penalize_info': seed_penalize_info,
        'first_seed_penalize_time': first_seed_penalize_time,
        'p60d_update_product_cnt': p60d_update_product_cnt,
        'strong_group_id': strong_group_id,
        'first_seed_grouprejectpenalize_time': first_seed_grouprejectpenalize_time
    }
print(len(shop_info_dict))

# %% [code]
group_id_list = group_data['group_id'].value_counts().index.tolist()
print(len(group_id_list), group_id_list[:3])

# %% [code]
group_output = []
for group_id in tqdm(group_id_list):
    temp_group_data = group_data[group_data['group_id']==group_id]
    origin_group_size = temp_group_data['group_size'].iloc[0]

    seed_shop_list = []
    unseed_shop_list = []
    worthpenalize_seed_shop_list = []
    for idx in range(len(temp_group_data)):
        shop_id = temp_group_data['shop_id'].iloc[idx]
        is_seed = temp_group_data['is_seed'].iloc[idx]
        seed_penalize_info = temp_group_data['seed_penalize_info'].iloc[idx]
        strong_group_id = temp_group_data['strong_group_id'].iloc[idx]
        p60d_update_product_cnt = temp_group_data['p60d_update_product_cnt'].iloc[idx]
        first_seed_grouprejectpenalize_time = temp_group_data['first_seed_grouprejectpenalize_time'].iloc[idx]
        if is_seed == 1:
            seed_shop_list.append(shop_id)
            if first_seed_grouprejectpenalize_time is None:
                worthpenalize_seed_shop_list.append(shop_id)
        else:
            if p60d_update_product_cnt >= 1:
                unseed_shop_list.append(shop_id)
    seed_shop_list = list(set(seed_shop_list))
    unseed_shop_list = list(set(unseed_shop_list))
    
    origin_group_seed_cnt = len(seed_shop_list)
    origin_group_seed_rate = float(origin_group_seed_cnt) / float(origin_group_size)

    # 聚集性过滤
    if len(seed_shop_list) <= 2:
        continue
    # 进审size过大切分
    if len(seed_shop_list) + len(unseed_shop_list) <= 10:
        group_shop_list = seed_shop_list + unseed_shop_list
        if len(list(set(group_shop_list) & set(worthpenalize_seed_shop_list))) == 0:
            continue
        group_output.append({'group_id': group_id, 'sub_group_id': -1, 'group_shop_list': group_shop_list, 'origin_group_size': origin_group_size, 'origin_group_seed_cnt': origin_group_seed_cnt, 'origin_group_seed_rate': origin_group_seed_rate})
    else:
        random.shuffle(seed_shop_list)
        seed_split_list = split_chunks(3, seed_shop_list)
        random.shuffle(unseed_shop_list)
        unseed_split_list = split_chunks(7, unseed_shop_list)
        sub_group_id = 20000
        g_idx = 0
        seed_end_sign = 0
        unseed_end_sign = 0
        while seed_end_sign == 0:
            if g_idx < len(seed_split_list) - 1:
                group_seed_shop_list = seed_split_list[g_idx]
            elif g_idx == len(seed_split_list) - 1:
                group_seed_shop_list = seed_split_list[-1] + random.sample([i for i in seed_shop_list if i not in seed_split_list[-1]], 3 - len(seed_split_list[-1]))
            else:
                group_seed_shop_list = random.sample(seed_shop_list, 3)
            if g_idx <= len(unseed_split_list) - 1:
                group_unseed_shop_list = unseed_split_list[g_idx]
            else:
                group_unseed_shop_list = []
            g_idx += 1
            if g_idx > len(seed_split_list) - 1:
                seed_end_sign = 1
            if g_idx > len(unseed_split_list) - 1:
                unseed_end_sign = 1
            group_shop_list = group_seed_shop_list + group_unseed_shop_list
            if len(list(set(group_shop_list) & set(worthpenalize_seed_shop_list))) == 0:
                continue
            group_output.append({'group_id': group_id, 'sub_group_id': sub_group_id, 'group_shop_list': group_shop_list, 'origin_group_size': origin_group_size, 'origin_group_seed_cnt': origin_group_seed_cnt, 'origin_group_seed_rate': origin_group_seed_rate})
            sub_group_id += 1
print(len(group_output))

# %% [code]
print(group_output[:3])

# %% [code]
rule_label_dict = {
    224287:	['底线问题-虚假宣传',	'虚假宣传-商家'],
    223626:	['底线问题-虚假宣传',	'虚假宣传-商家'],
    223625:	['底线问题-虚假宣传',	'虚假宣传-商家'],
    222141:	['底线问题-虚假宣传',	'虚假宣传-商家'],
    222166:	['底线问题-虚假宣传',	'虚假宣传-商家'],
    222178:	['底线问题-虚假宣传',	'虚假宣传-商家'],
    210005:	['底线问题-虚假宣传',	'虚假宣传-商家'],
    223290:	['底线问题-虚假宣传',	'虚假宣传-商家'],
    223291:	['底线问题-虚假宣传',	'虚假宣传-商家'],
    225524:	['底线问题-虚假宣传',	'虚假宣传-商家'],
    223292:	['底线问题-虚假宣传',	'虚假宣传-商家']
}

# %% [code]
class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.int64):
            return int(obj)
        return json.JSONEncoder.default(self, obj)

# %% [code]
group_result = []
for group_info in tqdm(group_output):
    recall_time = '{} 00:00:00'.format('${DATE}')
    recall_reason = '虚假宣传种子扩散'
    seed_shop_group_type = '虚假宣传'
    group_id = str(group_info['group_id'])
    sub_group_id = group_info['sub_group_id']
    new_group_id = '{}_{}'.format(group_id, sub_group_id)
    group_size = str(group_info['origin_group_size'])
    black_seed_rate = '{:.4f}'.format(group_info['origin_group_seed_rate'])
    shop_info_list = []
    for shop_id in group_info['group_shop_list']:
        shop_info = shop_info_dict[shop_id]
        is_seed = shop_info['is_seed']
        if is_seed == 1:
            black_seed_shop_type = '虚假宣传'
        else:
            black_seed_shop_type = ''
        seed_penalize_info_list = []
        if shop_info['seed_penalize_info'] is not None:
            for seed_penalize_info in shop_info['seed_penalize_info']:
                seed_penalize_info = json.loads(seed_penalize_info)
                rule_id = int(seed_penalize_info['rule_id'])
                penalize_id = int(seed_penalize_info['penalize_id'])
                penalize_product_id = int(seed_penalize_info['penalize_product_id'])
                penalize_reason = seed_penalize_info['penalize_reason']
                penalize_time = seed_penalize_info['penalize_time']
                penalize_first_label = rule_label_dict[rule_id][0]
                penalize_second_label = rule_label_dict[rule_id][1]
                seed_penalize_info_list.append({
                    'rule_id': rule_id,
                    'penalize_id': penalize_id,
                    'penalize_product_id': penalize_product_id,
                    'penalize_brand': '',
                    'penalize_first_label': penalize_first_label,
                    'penalize_second_label': penalize_second_label,
                    'penalize_reason': penalize_reason,
                    'penalize_time': penalize_time,
                })
        other_seed_shop_relation_list = []
        strong_group_id = shop_info['strong_group_id']
        for other_shop_id in group_info['group_shop_list']:
            if other_shop_id == shop_id or shop_info_dict[other_shop_id]['is_seed'] == 0:
                continue
            other_shop_strong_group_id = shop_info_dict[other_shop_id]['strong_group_id']
            if strong_group_id is not None and other_shop_strong_group_id is not None and strong_group_id == other_shop_strong_group_id:
                other_seed_shop_relation_list.append({
                    'seed_shop_id': other_shop_id,
                    'relation': 'highprec/strong'
                })
            else:
                other_seed_shop_relation_list.append({
                    'seed_shop_id': other_shop_id,
                    'relation': 'highprec'
                })
        shop_info_list.append({
            'shop_id': shop_id,
            'is_black_seed': is_seed,
            'black_seed_shop_type': black_seed_shop_type,
            'black_seed_info': seed_penalize_info_list,
            'seed_shop_relation': other_seed_shop_relation_list
        })
    group_result.append({
        'recall_time': recall_time,
        'recall_reason': recall_reason,
        'seed_shop_group_type': seed_shop_group_type,
        'strong_group_type': '',
        'group_id': group_id,
        'group_size': group_size,
        'black_seed_rate': black_seed_rate,
        'shop_infos': json.dumps(shop_info_list, cls=NumpyEncoder, ensure_ascii=False),
        'new_group_id': new_group_id
    })
print(len(group_result))

# %% [code]
group_result_df = pd.DataFrame(group_result)
group_result_df

# %% [code]
def dataframe2hive(df, table_name, date, risk_type, spark):
    output_df = spark.createDataFrame(df).repartition(2)
    output_df.createOrReplaceTempView("temp_df_output")
    save_sql = """
                INSERT OVERWRITE TABLE {} PARTITION(date = '{}', risk_type = '{}')
                select
                    recall_time,
                    recall_reason,
                    seed_shop_group_type,
                    strong_group_type,
                    group_id,
                    group_size,
                    black_seed_rate,
                    shop_infos,
                    new_group_id
                from
                    temp_df_output
    """.format(table_name, date, risk_type)
    spark.sql(save_sql)

# %% [code]
dataframe2hive(df=group_result_df, table_name='ecom_govern_community.groupaudit_shop_recallstrategy_v1_result_groupshopinfo', date=today, risk_type='xjxc', spark=spark)

# %% [code]


