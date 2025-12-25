# %% [code]
##建立sparkSession
# coding=utf-8
from pyspark.sql import SparkSession
import pandas as pd
import numpy as np
from tqdm import tqdm
import os
import time
from datetime import datetime
from collections import defaultdict
import re
import random
import json
spark = SparkSession.builder.enableHiveSupport().getOrCreate()
sc = spark.sparkContext
today = str(${date})
sql = spark.sql
today_placeholder = '$[date]'
print("today: {}".format(today))

# %% [code]
shop_jh_seed_penalize_sql = f"""
SELECT  A.shop_id,
        A.rule_id,
        A.ticket_id AS penalize_id,
        A.ticket_create_time AS penalize_time,
        A.subject_id,
        A.object_id,
        B.final_factor_info_list_map
FROM    ecom.dwd_gvn_penalty_ticket_info_df AS A
LEFT JOIN
        ecom.dwd_gvn_penalty_judge_record_df AS B
ON      A.uuid=B.uuid
AND     B.date='${date}'
WHERE   A.date='${date}'
AND     A.status=1
AND     A.revoke_way NOT IN (1, 2, 3)
AND     A.rule_id IN (216941, 216949, 219466, 219472, 220715, 220713, 219468, 219469, 219470, 219471, 212450, 219467, 220714, 208719, 208721)
AND     A.ticket_create_time>='${DATE-59}'
AND     A.ticket_create_time<'${DATE+1}'
AND     A.ticket_type_code=3
AND     A.is_deleted=0
"""
shop_jh_seed_penalize=sql(shop_jh_seed_penalize_sql).toPandas()
print(len(shop_jh_seed_penalize))
print(shop_jh_seed_penalize[:3])

# %% [code]
# 216941 violation_detail:commodity_id 多个商品ID逗号拼接,解析成商品列表,即一个罚单对应多个商品
# 216949 violation_detail:commodity_id 多个商品ID逗号拼接,解析成商品列表,即一个罚单对应多个商品,部分数据无法解析(violation_detail:commodity_id为中文)
# 219466 violation_detail:product_id 多个商品ID逗号拼接,解析成商品列表,即一个罚单对应多个商品
# 219472 violation_detail:product_id 多个商品ID逗号拼接,解析成商品列表,即一个罚单对应多个商品,部分数据带有双引号需要额外处理
# 220715 无
# 220713 violation_detail:commodity_id 多个商品ID逗号拼接,解析成商品列表,即一个罚单对应多个商品
# 219468 violation_detail:product_id 多个商品ID逗号拼接,解析成商品列表,即一个罚单对应多个商品
# 219469 violation_detail:product_id 多个商品ID逗号拼接,解析成商品列表,即一个罚单对应多个商品
# 219470 violation_detail:product_id 多个商品ID逗号拼接,解析成商品列表,即一个罚单对应多个商品
# 219471 violation_detail:product_id 多个商品ID逗号拼接,解析成商品列表,即一个罚单对应多个商品
# 212450 无
# 219467 无
# 220714 无
# 208719 无
# 208721 无

# %% [code]
output = []
for idx in tqdm(range(len(shop_jh_seed_penalize))):
    shop_id = shop_jh_seed_penalize['shop_id'].iloc[idx]
    rule_id = shop_jh_seed_penalize['rule_id'].iloc[idx]
    penalize_id = shop_jh_seed_penalize['penalize_id'].iloc[idx]
    penalize_time = shop_jh_seed_penalize['penalize_time'].iloc[idx]
    final_factor_info_list_map = shop_jh_seed_penalize['final_factor_info_list_map'].iloc[idx]

    product_id_list = []
    try:
        recall_product_verify_result = final_factor_info_list_map.get('recall_product_verify_result', None)
        recall_product_verify_result = json.loads(recall_product_verify_result)
        verified_order_info = recall_product_verify_result.get('verified_order_info', None)
        for verified_order in verified_order_info:
            if len(verified_order['verify_reason']) >= 1:
                product_id_sentence = verified_order['product_id'].replace(' ', '').replace('"', '').replace("'", '').replace('-', ',')
                product_id_list += [int(i) for i in product_id_sentence.split(',')]
    except Exception as e:
        pass
    try:
        violation_detail_commodity_id = final_factor_info_list_map.get('violation_detail:commodity_id', None)
        violation_detail_commodity_id = violation_detail_commodity_id.replace(' ', '').replace('"', '').replace("'", '').replace('-', ',')
        product_id_list += [int(i) for i in violation_detail_commodity_id.split(',')]
    except Exception as e:
        pass
    try:
        violation_detail_product_id = final_factor_info_list_map.get('violation_detail:product_id', None)
        violation_detail_product_id = violation_detail_product_id.replace(' ', '').replace('"', '').replace("'", '').replace('-', ',')
        product_id_list += [int(i) for i in violation_detail_product_id.split(',')]
    except Exception as e:
        pass
    
    product_id_list = list(set(product_id_list))
    if len(product_id_list) >= 1:
        for product_id in product_id_list:
            output.append({'shop_id': shop_id, 'rule_id': rule_id, 'penalize_id': penalize_id, 'penalize_time': penalize_time, 'order_id': -1, 'product_id': product_id})

# %% [code]
output_df = pd.DataFrame(output)
output_df

# %% [code]
def dataframe2hive(df, table_name, date, risk_type, spark):
    output_df = spark.createDataFrame(df).repartition(2)
    output_df.createOrReplaceTempView("temp_df_output")
    save_sql = """
                INSERT OVERWRITE TABLE {} PARTITION(date = '{}', risk_type = '{}')
                select
                    shop_id,
                    rule_id,
                    penalize_id,
                    penalize_time,
                    order_id,
                    product_id
                from
                    temp_df_output
    """.format(table_name, date, risk_type)
    spark.sql(save_sql)

# %% [code]
dataframe2hive(df=output_df, table_name='ecom_govern_community.groupaudit_shop_seed_penalize_product_info', date=today, risk_type='jh', spark=spark)

# %% [code]


