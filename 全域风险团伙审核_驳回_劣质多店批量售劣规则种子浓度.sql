-- ********************************************************************
-- Author: huangaobin.milotic
-- CreateTime: 2025-10-23 16:40:33
-- Description:
-- Update: Task Update Description
-- ********************************************************************
WITH all_audit_data AS (
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
            strong_group_id
    FROM    ecom_govern_community.groupaudit_shop_recallstrategy_v1_filter_groupshopinfo
    WHERE   date = '${date}'
    AND     risk_type = 'lz'
),
group_lz_seed_rate AS (
    SELECT  group_id,
            count(CASE WHEN is_seed = 1 THEN shop_id ELSE NULL END) / count(
                CASE WHEN is_seed = 1 OR is_alive = 1 THEN shop_id
                     ELSE NULL
                END
            ) AS seed_rate,
            count(CASE WHEN is_seed = 1 THEN shop_id ELSE NULL END) AS seed_cnt,
            COLLECT_LIST(CASE WHEN is_seed = 1 THEN shop_id ELSE NULL END) AS seed_list,
            COLLECT_LIST(
                CASE WHEN is_seed = 0 AND is_alive = 1 THEN shop_id
                     ELSE NULL
                END
            ) AS other_alive_list
    FROM    (
                SELECT  A.group_id,
                        A.shop_id,
                        CASE WHEN B.shop_id IS NOT NULL THEN 1 ELSE 0 END AS is_seed,
                        CASE WHEN C.shop_id IS NOT NULL THEN 1 ELSE 0 END AS is_alive
                FROM    ecom_govern_community.groupaudit_shop_recallstrategy_v1_origin_groupshopinfo AS A
                LEFT JOIN
                        (
                            SELECT  DISTINCT shop_id
                            FROM    ecom.dwd_gvn_penalty_ticket_info_df
                            WHERE   date = '${date}'
                            AND     status = 1
                            AND     revoke_way NOT IN (1, 2, 3)
                            AND     ticket_create_time >= '${DATE-89}'
                            AND     ticket_create_time < '${DATE+1}'
                            AND     rule_id IN (1912, 1914, 1916)
                            AND     ticket_type_code = 3
                            AND     is_deleted = 0
                        ) AS B
                ON      A.shop_id = B.shop_id
                LEFT JOIN
                        (
                            SELECT  DISTINCT shop_id
                            FROM    dm_temai.shop_ip_link_daily_v2
                            WHERE   date >= '${date-29}'
                            AND     date <= '${date}'
                            UNION
                            SELECT  DISTINCT shop_id
                            FROM    dm_temai.shop_did_link_daily_v2
                            WHERE   date >= '${date-29}'
                            AND     date <= '${date}'
                        ) AS C
                ON      A.shop_id = C.shop_id
                WHERE   A.date = '${date}'
            )
    GROUP BY
            group_id
    HAVING  seed_cnt >= 2
)
INSERT OVERWRITE TABLE ecom_govern_community.groupaudit_shop_rejectfactor_lzmulti_seedrate PARTITION (date = '${date}')
SELECT  A.group_id,
        A.shop_id,
        A.is_seed,
        B.seed_rate,
        B.seed_list,
        B.other_alive_list
FROM    all_audit_data AS A
INNER JOIN
        group_lz_seed_rate AS B
ON      A.group_id = B.group_id
WHERE   A.is_seed = 1