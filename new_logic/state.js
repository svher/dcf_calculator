/**
 * Global State Management
 */
const State = {
    currentFile: null,      // Current open file path
    currentNodeId: null,    // Current selected graph node ID
    currentMode: 'simplified', // 'simplified' or 'original' (though we might drop this concept in favor of just file view)

    // Mapping from Graph Node ID to File Info
    nodeMapping: {
        "SummaryScript": {
            filename: "全域风险团伙审核_召回_汇总.py",
            lang: "python",
            path: "../全域风险团伙审核_召回_汇总.py"
        },
        "RuleScript": {
            filename: "全域风险团伙审核_召回_团伙规则过滤_虚假宣传.py", // Assuming this is the representative one
            lang: "python",
            path: "../全域风险团伙审核_召回_团伙规则过滤_虚假宣传.py"
        },
        "FilterSQL": {
            filename: "全域风险团伙审核_召回_团伙种子过滤_虚假宣传.sql",
            lang: "sql",
            path: "../全域风险团伙审核_召回_团伙种子过滤_虚假宣传.sql"
        },
        "BaseDataSQL": {
            filename: "全域风险团伙审核_召回_团伙基础数据.sql",
            lang: "sql",
            path: "../全域风险团伙审核_召回_团伙基础数据.sql"
        },
        "ParseScript": {
            filename: "底线违规商品映射解析_假货.py",
            lang: "python",
            path: "../底线违规商品映射解析_假货.py"
        }
    },

    // Navigation keywords for jumping to specific code blocks
    codeNavigation: {
        "SummaryScript": {
            "LoadToday": "today_data_sql = \"\"\"",
            "LoadHistory": "history_data_sql = \"\"\"",
            "Loop": "for idx in tqdm(range(len(temp_today_data))):",
            "CheckSet": "if set(seed_shop_list) <= set(history_link_seed_list):",
            "SaveHive": "dataframe2hive"
        },
        "RuleScript": {
            "Load": "group_data_sql = \"\"\"",
            "Group": "for group_id in tqdm(group_id_list):",
            "Sort": "random.shuffle(seed_shop_list)",
            "CheckSize": "if len(seed_shop_list) + len(unseed_shop_list) <= 10:",
            "Split": "split_chunks(",
            "Keep": "group_shop_list = seed_shop_list + unseed_shop_list",
            "BuildJSON": "group_result.append({",
            "SaveHive": "dataframe2hive(df=group_result_df"
        },
        "FilterSQL": {
            "Origin": "FROM    ecom_govern_community.groupaudit_shop_recallstrategy_v1_origin_groupshopinfo AS A",
            "Penalty": "AND     rule_id IN (216949)",
            "Window1": "AS group_jh_seed_cnt",
            "Window2": "AS group_has_auditproduct_shop_cnt",
            "Window3": "AS group_worth_reejctandpenalize_jh_seed_cnt",
            "Filter": "WHERE   group_jh_seed_cnt >= 1",
            "Insert": "INSERT OVERWRITE TABLE ecom_govern_community.groupaudit_shop_recallstrategy_v1_filter_groupshopinfo"
        },
        "BaseDataSQL": {
            "CTE1": "WITH group_base_data AS (",
            "CTE2": "shop_main_cate as (",
            "CTE3": "seed_penliaze_data AS (",
            "Join1": "B1.risk_type = 'jh'",
            "Join2": "B2.risk_type = 'lz'",
            "Join3": ") as B3 on A.shop_id = B3.shop_id",
            "Join4": "count(prod_id) AS product_cnt",
            "Join5": "dm_temai.shop_order_order_data",
            "Join6": "dm_temai.shop_same_people_shops_group_hard_lab",
            "Insert": "INSERT OVERWRITE TABLE ecom_govern_community.groupaudit_shop_recallstrategy_v1_origin_groupshopinfo"
        },
        "ParseScript": {
            "Query": "shop_jh_seed_penalize_sql =",
            "Loop": "for idx in tqdm(range(len(shop_jh_seed_penalize)))",
            "Try1": "recall_product_verify_result = final_factor_info_list_map.get",
            "Try2": "violation_detail_commodity_id = final_factor_info_list_map.get",
            "Try3": "violation_detail_product_id = final_factor_info_list_map.get",
            "Dedupe": "product_id_list = list(set(product_id_list))",
            "Expand": "for product_id in product_id_list:",
            "SaveHive": "dataframe2hive"
        }
    }
};
