-- ********************************************************************
-- Author: huangaobin.milotic
-- CreateTime: 2025-08-26 11:57:33
-- Description:
-- Update: Task Update Description
-- ********************************************************************
INSERT OVERWRITE TABLE ecom_govern_community.groupaudit_shop_recallstrategy_v1_filter_groupshopinfo PARTITION (date = '${date}', risk_type = 'xjxc')
SELECT  group_type,
        group_id,
        group_size,
        shop_id,
        shop_type_code,
        operate_status_code,
        stop_status_code,
        shop_open_time,
        is_xjxc_seed AS is_seed,
        xjxc_seed_penalize_info AS seed_penalize_info,
        xjxc_first_seed_penalize_time AS first_seed_penalize_time,
        product_cnt,
        p60d_update_product_cnt,
        latest_product_update_time,
        p60d_create_product_cnt,
        latest_product_create_time,
        p60d_pay_order_cnt,
        p30d_pay_order_cnt,
        strong_group_id,
        first_seed_grouprejectpenalize_time
FROM    (
            SELECT  A.group_type,
                    A.group_id,
                    A.group_size,
                    A.shop_id,
                    A.shop_type_code,
                    A.operate_status_code,
                    A.stop_status_code,
                    A.shop_open_time,
                    A.is_xjxc_seed,
                    A.xjxc_seed_penalize_info,
                    A.xjxc_first_seed_penalize_time,
                    A.product_cnt,
                    A.p60d_update_product_cnt,
                    A.latest_product_update_time,
                    A.p60d_create_product_cnt,
                    A.latest_product_create_time,
                    A.p60d_pay_order_cnt,
                    A.p30d_pay_order_cnt,
                    A.strong_group_id,
                    B.first_seed_grouprejectpenalize_time,
                    count(CASE WHEN A.is_xjxc_seed = 1 THEN A.shop_id ELSE NULL END) OVER (
                        PARTITION BY
                                A.group_type,
                                A.group_id
                    ) AS group_xjxc_seed_cnt,
                    count(
                        CASE WHEN A.is_xjxc_seed = 1 OR A.p60d_update_product_cnt >= 1 THEN A.shop_id
                             ELSE NULL
                        END
                    ) OVER (
                        PARTITION BY
                                A.group_type,
                                A.group_id
                    ) AS group_has_auditproduct_shop_cnt,
                    count(
                        CASE WHEN A.is_xjxc_seed = 1 AND B.first_seed_grouprejectpenalize_time IS NULL THEN A.shop_id
                             ELSE NULL
                        END
                    ) OVER (
                        PARTITION BY
                                A.group_type,
                                A.group_id
                    ) AS group_worth_reejctandpenalize_xjxc_seed_cnt
            FROM    ecom_govern_community.groupaudit_shop_recallstrategy_v1_origin_groupshopinfo AS A
            LEFT JOIN
                    (
                        SELECT  shop_id,
                                MIN(ticket_create_time) AS first_seed_grouprejectpenalize_time
                        FROM    ecom.dwd_gvn_penalty_ticket_info_df
                        WHERE   date = '${date}'
                        AND     status = 1
                        AND     revoke_way NOT IN (1, 2, 3)
                        AND     ticket_type_code = 3
                        AND     is_deleted = 0
                        AND     rule_id IN (224286)
                        AND     ticket_create_time >= '${DATE-59}'
                        AND     ticket_create_time < '${DATE+1}'
                        GROUP BY
                                shop_id
                    ) AS B
            ON      A.shop_id = B.shop_id
            WHERE   A.date = '${date}'
        )
WHERE   group_xjxc_seed_cnt >= 3
AND     group_worth_reejctandpenalize_xjxc_seed_cnt >= 1
AND     group_has_auditproduct_shop_cnt >= 3