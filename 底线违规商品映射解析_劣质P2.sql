-- ********************************************************************
-- Author: huangaobin.milotic
-- CreateTime: 2025-08-24 17:43:49
-- Description:
-- Update: Task Update Description
-- ********************************************************************
INSERT OVERWRITE TABLE ecom_govern_community.groupaudit_shop_seed_penalize_product PARTITION (date = '${date}', risk_type = 'lz')
SELECT  shop_id,
        rule_id,
        penalize_id,
        penalize_time,
        product_id
FROM    ecom_govern_community.groupaudit_shop_seed_penalize_product_info
WHERE   date = '${date}'
AND     risk_type = 'lz'
AND     product_id > 0