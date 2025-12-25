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
today_data_sql = """
SELECT  recall_time,
        recall_reason,
        seed_shop_group_type,
        strong_group_type,
        group_id,
        group_size,
        black_seed_rate,
        shop_infos,
        new_group_id
FROM    ecom_govern_community.groupaudit_shop_recallstrategy_v1_result_groupshopinfo
WHERE   date = '${date}'
AND     risk_type IN ('jh', 'lz', 'xjxc')
"""
today_data=sql(today_data_sql).toPandas()
print(len(today_data))
today_data

# %% [code]
history_data_sql = """
SELECT  recall_time,
        recall_reason,
        seed_shop_group_type,
        strong_group_type,
        group_id,
        group_size,
        black_seed_rate,
        shop_infos,
        new_group_id
FROM    ecom_govern_community.groupaudit_shop_recallstrategy_v1_result_groupshopinfo
WHERE   date >= '${date-7}' and date <= '${date-1}'
AND     risk_type IN ('jh', 'lz', 'xjxc')
"""
history_data=sql(history_data_sql).toPandas()
print(len(history_data))
history_data

# %% [code]
temp_today_data = today_data[today_data['seed_shop_group_type']=='假货']
temp_history_data = history_data[history_data['seed_shop_group_type']=='假货']
history_shop_link_seed_dict = {}
for idx in tqdm(range(len(temp_history_data))):
    group_shop_info = eval(temp_history_data['shop_infos'].iloc[idx])
    seed_shop_list = []
    group_shop_list = []
    for shop_info in group_shop_info:
        shop_id = shop_info['shop_id']
        is_black_seed = shop_info['is_black_seed']
        if is_black_seed == 1:
            seed_shop_list.append(shop_id)
        group_shop_list.append(shop_id)
    for shop_id in group_shop_list:
        if shop_id not in history_shop_link_seed_dict:
            history_shop_link_seed_dict[shop_id] = []
        history_shop_link_seed_dict[shop_id].append(seed_shop_list)
valid_idx_list = []
for idx in tqdm(range(len(temp_today_data))):
    group_shop_info = eval(temp_today_data['shop_infos'].iloc[idx])
    seed_shop_list = []
    group_shop_list = []
    for shop_info in group_shop_info:
        shop_id = shop_info['shop_id']
        is_black_seed = shop_info['is_black_seed']
        if is_black_seed == 1:
            seed_shop_list.append(shop_id)
        group_shop_list.append(shop_id)
    valid_shop_list = []
    for shop_id in group_shop_list:
        filter_sign = 0
        if shop_id in history_shop_link_seed_dict:
            for history_link_seed_list in history_shop_link_seed_dict[shop_id]:
                if set(seed_shop_list) <= set(history_link_seed_list):
                    filter_sign = 1
                    break
        if filter_sign == 0:
            valid_shop_list.append(shop_id)
    if len(valid_shop_list) > 0:
        valid_idx_list.append(idx)
print(len(valid_idx_list), valid_idx_list[:3])
valid_jh_data = temp_today_data.iloc[valid_idx_list]

# %% [code]
temp_today_data = today_data[today_data['seed_shop_group_type']=='劣质']
temp_history_data = history_data[history_data['seed_shop_group_type']=='劣质']
history_shop_link_seed_dict = {}
for idx in tqdm(range(len(temp_history_data))):
    group_shop_info = eval(temp_history_data['shop_infos'].iloc[idx])
    seed_shop_list = []
    group_shop_list = []
    for shop_info in group_shop_info:
        shop_id = shop_info['shop_id']
        is_black_seed = shop_info['is_black_seed']
        if is_black_seed == 1:
            seed_shop_list.append(shop_id)
        group_shop_list.append(shop_id)
    for shop_id in group_shop_list:
        if shop_id not in history_shop_link_seed_dict:
            history_shop_link_seed_dict[shop_id] = []
        history_shop_link_seed_dict[shop_id].append(seed_shop_list)
valid_idx_list = []
for idx in tqdm(range(len(temp_today_data))):
    group_shop_info = eval(temp_today_data['shop_infos'].iloc[idx])
    seed_shop_list = []
    group_shop_list = []
    for shop_info in group_shop_info:
        shop_id = shop_info['shop_id']
        is_black_seed = shop_info['is_black_seed']
        if is_black_seed == 1:
            seed_shop_list.append(shop_id)
        group_shop_list.append(shop_id)
    valid_shop_list = []
    for shop_id in group_shop_list:
        filter_sign = 0
        if shop_id in history_shop_link_seed_dict:
            for history_link_seed_list in history_shop_link_seed_dict[shop_id]:
                if set(seed_shop_list) <= set(history_link_seed_list):
                    filter_sign = 1
                    break
        if filter_sign == 0:
            valid_shop_list.append(shop_id)
    if len(valid_shop_list) > 0:
        valid_idx_list.append(idx)
print(len(valid_idx_list), valid_idx_list[:3])
valid_lz_data = temp_today_data.iloc[valid_idx_list]

# %% [code]
temp_today_data = today_data[today_data['seed_shop_group_type']=='虚假宣传']
temp_history_data = history_data[history_data['seed_shop_group_type']=='虚假宣传']
history_shop_link_seed_dict = {}
for idx in tqdm(range(len(temp_history_data))):
    group_shop_info = eval(temp_history_data['shop_infos'].iloc[idx])
    seed_shop_list = []
    group_shop_list = []
    for shop_info in group_shop_info:
        shop_id = shop_info['shop_id']
        is_black_seed = shop_info['is_black_seed']
        if is_black_seed == 1:
            seed_shop_list.append(shop_id)
        group_shop_list.append(shop_id)
    for shop_id in group_shop_list:
        if shop_id not in history_shop_link_seed_dict:
            history_shop_link_seed_dict[shop_id] = []
        history_shop_link_seed_dict[shop_id].append(seed_shop_list)
valid_idx_list = []
for idx in tqdm(range(len(temp_today_data))):
    group_shop_info = eval(temp_today_data['shop_infos'].iloc[idx])
    seed_shop_list = []
    group_shop_list = []
    for shop_info in group_shop_info:
        shop_id = shop_info['shop_id']
        is_black_seed = shop_info['is_black_seed']
        if is_black_seed == 1:
            seed_shop_list.append(shop_id)
        group_shop_list.append(shop_id)
    valid_shop_list = []
    for shop_id in group_shop_list:
        filter_sign = 0
        if shop_id in history_shop_link_seed_dict:
            for history_link_seed_list in history_shop_link_seed_dict[shop_id]:
                if set(seed_shop_list) <= set(history_link_seed_list):
                    filter_sign = 1
                    break
        if filter_sign == 0:
            valid_shop_list.append(shop_id)
    if len(valid_shop_list) > 0:
        valid_idx_list.append(idx)
print(len(valid_idx_list), valid_idx_list[:3])
valid_xjxc_data = temp_today_data.iloc[valid_idx_list]

# %% [code]
valid_data = pd.concat([valid_jh_data, valid_lz_data, valid_xjxc_data], axis=0)

# %% [code]
valid_data

# %% [code]
in_data = valid_data
out_data = valid_data
print(len(in_data), len(out_data))

# %% [code]
def dataframe2hive(df, table_name, date, spark):
    output_df = spark.createDataFrame(df).repartition(2)
    output_df.createOrReplaceTempView("temp_df_output")
    save_sql = """
                INSERT OVERWRITE TABLE {} PARTITION(date = '{}')
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
    """.format(table_name, date)
    spark.sql(save_sql)

# %% [code]
dataframe2hive(df=in_data, table_name='ecom_govern_community.groupaudit_shop_recallstrategy_v1_groupshopinfo', date=today, spark=spark)

# %% [code]
seedrate_data_sql = f"""
SELECT  group_id,
        shop_id,
        is_seed,
        seed_rate,
        seed_list,
        other_alive_list
FROM    ecom_govern_community.groupaudit_shop_rejectfactor_lzmulti_seedrate
WHERE   date = '${date}'
"""
seedrate_data=sql(seedrate_data_sql).toPandas()
print(len(seedrate_data))
seedrate_data

# %% [code]
shop_seedrate_dict = {}
for idx in tqdm(range(len(seedrate_data))):
    shop_id = seedrate_data['shop_id'].iloc[idx]
    is_seed = seedrate_data['is_seed'].iloc[idx]
    seed_rate = seedrate_data['seed_rate'].iloc[idx]
    seed_list = seedrate_data['seed_list'].iloc[idx]
    other_alive_list = seedrate_data['other_alive_list'].iloc[idx]
    if is_seed == 1:
        shop_seedrate_dict[shop_id] = {
            'seed_rate': seed_rate,
            'seed_list': seed_list,
            'other_alive_list': other_alive_list
        }
print(len(shop_seedrate_dict))

# %% [code]
out_lz_data = valid_lz_data

# %% [code]
lz_multi_output = []
for idx in tqdm(range(len(out_lz_data))):
    group_id = out_lz_data['group_id'].iloc[idx]
    new_group_id = out_lz_data['new_group_id'].iloc[idx]
    group_size = out_lz_data['group_size'].iloc[idx]

    group_shop_info = today_data['shop_infos'].iloc[idx]
    group_shop_info = eval(group_shop_info)

    group_hit_sign = 0
    group_hit_seed_list = []
    group_hit_other_alive_list = []
    for shop_info in group_shop_info:
        shop_id = shop_info['shop_id']
        if shop_id in shop_seedrate_dict:
            seed_rate = shop_seedrate_dict[shop_id]['seed_rate']
            if seed_rate >= 0.5:
                group_hit_sign = 1
                group_hit_seed_list = shop_seedrate_dict[shop_id]['seed_list']
                group_hit_other_alive_list = shop_seedrate_dict[shop_id]['other_alive_list']
                break
    
    if group_hit_sign == 1:
        audit_shop_list = []
        shop_seedproduct_dict = {}
        for shop_info in group_shop_info:
            shop_id = shop_info['shop_id']
            audit_shop_list.append(shop_id)
            is_seed = shop_info['is_black_seed']
            if is_seed == 1:
                seed_product_list = []
                black_seed_info_list = shop_info['black_seed_info']
                for black_seed_info in black_seed_info_list:
                    seed_product_id = black_seed_info['penalize_product_id']
                    seed_product_list.append(seed_product_id)
                seed_product_list = list(set(seed_product_list))
                shop_seedproduct_dict[shop_id] = seed_product_list
        group_hit_seed_list = group_hit_seed_list
        group_hit_other_alive_list = group_hit_other_alive_list
        for shop_id in audit_shop_list:
            if shop_id in group_hit_seed_list and shop_id in shop_seedproduct_dict:
                is_seed = 1
                seed_product_list = shop_seedproduct_dict[shop_id]
            else:
                is_seed = 0
                seed_product_list = []
            lz_multi_output.append({
                'group_id': group_id,
                'group_size': group_size,
                'new_group_id': new_group_id,
                'shop_id': shop_id,
                'is_seed': is_seed,
                'seed_product_list': seed_product_list,
                'group_hit_seed_list': group_hit_seed_list,
                'group_hit_other_alive_list': group_hit_other_alive_list
            })

# %% [code]
print(len(lz_multi_output))

# %% [code]
lz_multi_output_df = pd.DataFrame(lz_multi_output)
lz_multi_output_df

# %% [code]
def dataframe2hive(df, table_name, date, spark):
    output_df = spark.createDataFrame(df).repartition(2)
    output_df.createOrReplaceTempView("temp_df_output")
    save_sql = """
                INSERT OVERWRITE TABLE {} PARTITION(date = '{}')
                select
                    cast(group_id as bigint) as group_id,
                    cast(group_size as bigint) as group_size,
                    new_group_id as audit_group_devide,
                    shop_id,
                    is_seed,
                    cast(seed_product_list as string) as seed_product_list,
                    cast(group_hit_seed_list as string) as group_hit_seed_list,
                    cast(group_hit_other_alive_list as string) as group_hit_other_alive_list
                from
                    temp_df_output
    """.format(table_name, date)
    spark.sql(save_sql)

# %% [code]
try:
    dataframe2hive(df=lz_multi_output_df, table_name='ecom_govern_community.groupaudit_shop_recallstrategy_v1_lzmulti_supply', date=today, spark=spark)
except Exception as e:
    print('ERROR: {}'.format(e))

# %% [code]


