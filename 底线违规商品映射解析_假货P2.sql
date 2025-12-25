-- ********************************************************************
-- Author: huangaobin.milotic
-- CreateTime: 2025-08-24 17:43:49
-- Description:
-- Update: Task Update Description
-- ********************************************************************
INSERT OVERWRITE TABLE ecom_govern_community.groupaudit_shop_seed_penalize_product PARTITION (date = '${date}', risk_type = 'jh')
SELECT  shop_id,
        rule_id,
        penalize_id,
        penalize_time,
        product_id
FROM    ecom_govern_community.groupaudit_shop_seed_penalize_product_info
WHERE   date = '${date}'
AND     risk_type = 'jh'
AND     product_id > 0
UNION
SELECT  A.shop_id,
        A.rule_id,
        A.ticket_id AS penalize_id,
        A.ticket_create_time AS penalize_time,
        B.product_id
FROM    ecom.dwd_gvn_penalty_ticket_info_df AS A
INNER JOIN
        ecom.dm_aftersale_order_status_df AS B
ON      A.object_id = B.order_id
AND     B.date = '${date}'
WHERE   A.date = '${date}'
AND     A.status = 1
AND     A.revoke_way NOT IN (1, 2, 3)
AND     A.rule_id IN (221454, 220086)
AND     A.ticket_create_time >= '${DATE-59}'
AND     A.ticket_create_time < '${DATE+1}'
AND     A.ticket_type_code = 3
AND     A.is_deleted = 0