-- ********************************************************************
-- Author: huangaobin.milotic
-- CreateTime: 2025-08-25 19:52:40
-- Description:
-- Update: Task Update Description
-- ********************************************************************
-- 高准团伙信息
WITH group_base_data AS (
    SELECT  group_type,
            group_id,
            group_size,
            shop_id,
            shop_type_code,
            operate_status_code,
            stop_status_code,
            shop_open_time
    FROM    (
                SELECT  'highprec' AS group_type,
                        A.group_id,
                        count(A.shop_id) OVER (
                            PARTITION BY
                                    A.group_id
                        ) AS group_size,
                        A.shop_id,
                        B.shop_type_code,
                        B.operate_status_code,
                        B.stop_status_code,
                        B.first_normal_business_time as shop_open_time
                FROM    dm_temai.shop_fusion_link_group_v1 AS A
                LEFT JOIN
                        ecom.dim_slr_shop_ext_df AS B
                ON      A.shop_id=B.shop_id
                AND     B.date='${date}'
                WHERE   A.date='${date-1}'
                AND     A.group_size>=3
                AND     B.operate_status_code IN (0, 1)
                AND     B.shop_type_code NOT IN (12, 13)
            )
    WHERE   group_size>=3
),
shop_main_cate as (
    with shop_cate_order_stat as (
        SELECT  shop_id,
                first_cate,
                second_cate,
                sum(pay_gmv) AS pay_gmv
        FROM    (
                    SELECT  B.shop_id,
                            A.total_amount AS pay_gmv,
                            A.first_name_new AS first_cate,
                            A.second_name_new AS second_cate,
                            A.order_id
                    FROM    ecom.dm_aftersale_order_status_df AS A
                    INNER   JOIN
                            ecom.dim_slr_shop_ext_df AS B
                    ON      A.shop_id=B.shop_id
                    AND     B.operate_status_code IN (0, 1)
                    AND     B.date='${date}'
                    WHERE   A.date='${date}'
                    AND     A.if_abnormal=0
                    AND     A.normal_pay_order_cnt=1
                    AND     from_unixtime(A.stockup_time, 'yyyyMMdd')>='${date-89}'
                    AND     from_unixtime(A.stockup_time, 'yyyyMMdd')<='${date}'
                    AND     B.shop_type_code NOT IN (12, 13)
                )
        GROUP BY
                shop_id,
                first_cate,
                second_cate
    ),
    main_secondcate as (
        SELECT  shop_id,
                second_cate
        FROM    (
                    SELECT  shop_id,
                            second_cate,
                            pay_gmv,
                            row_number() OVER (
                                PARTITION BY
                                        shop_id
                                ORDER BY
                                        pay_gmv DESC
                            ) AS pay_gmv_d_order
                    FROM    shop_cate_order_stat
                )
        WHERE   pay_gmv_d_order=1
    ),
    main_firstcate as (
        SELECT  shop_id,
                first_cate
        FROM    (
                    SELECT  shop_id,
                            first_cate,
                            pay_gmv,
                            row_number() OVER (
                                PARTITION BY
                                        shop_id
                                ORDER BY
                                        pay_gmv DESC
                            ) AS pay_gmv_d_order
                    FROM    shop_cate_order_stat
                )
        WHERE   pay_gmv_d_order=1
    )
    SELECT  shop_id,
            CASE
                WHEN main_second_cate LIKE '%跨境%' THEN 'G2快消跨境'
                WHEN main_firstcate IN (
                    'DIY电脑',
                    '电脑组件',
                    '电子元器件市场',
                    '行业服务权益',
                    '投影',
                    '手机',
                    '办公设备/耗材/相关服务',
                    '平板电脑/MID',
                    '智能设备',
                    '大家电',
                    '笔记本电脑',
                    '摄影/摄像',
                    '网络设备/网络相关',
                    '电玩/配件/游戏/攻略',
                    '电脑外设',
                    '品牌台机/品牌一体机/服务器',
                    '3C数码配件',
                    '影音电器',
                    '数码存储设备',
                    '电子教育',
                    '房产',
                    '兑换卡',
                    '购物提货券',
                    '本地生活服务',
                    '电影/演出/赛事',
                    '二手3C数码',
                    '新车',
                    '保险',
                    '游戏服务',
                    '生活娱乐充值',
                    '移动/联通/电信充值中心',
                    '手机号码/套餐/增值业务',
                    '有价券',
                    '橡塑材料及制品',
                    '搬运/仓储/物流设备',
                    '标准件/零部件/工业耗材',
                    '包装',
                    '润滑/胶粘/试剂/实验室耗材',
                    '自行车/骑行装备/零配件',
                    '电动车/配件/交通工具',
                    '基础建材',
                    '五金/工具',
                    '电子/电工',
                    '个人保健/护理电器',
                    '家装主材',
                    '全屋定制',
                    '住宅家具',
                    '金属材料及制品',
                    '汽车零配件',
                    '装修设计/施工/监理',
                    '厨房电器',
                    '家装灯饰光源',
                    '生活电器',
                    '摩托车/装备/配件',
                    '清洗/食品/商业设备',
                    '机械设备',
                    '商业/办公家具',
                    '汽车用品',
                    '全屋智能'
                ) THEN 'G3耐消与虚拟'
                WHEN main_firstcate IN (
                    '教育培训',
                    '洗护清洁剂/卫生巾/纸/香薰',
                    '个人护理',
                    '酒类',
                    '节庆用品/文创',
                    '美容护肤',
                    '美容美体医疗器械',
                    '美容/个护仪器',
                    '彩妆/香水/美妆工具',
                    '儿童床品/家纺',
                    '婴童尿裤',
                    '孕产妇服纺/用品/营养',
                    '婴童用品',
                    '宠物/宠物食品及用品',
                    '奶粉/辅食/营养品/零食',
                    '学习用品/办公用品',
                    '教育音像',
                    '书籍/杂志/报纸',
                    '模玩/动漫/周边/娃圈/三坑/桌游',
                    '玩具/童车/益智/积木/模型',
                    '乐器/吉他/钢琴/配件',
                    '传统滋补营养品',
                    '保健食品/膳食营养补充食品'
                ) THEN 'G2快消跨境'
                WHEN main_firstcate IN (
                    '女装',
                    '服饰配件/皮带/帽子/围巾',
                    '内衣裤袜',
                    '男装',
                    '童装/婴儿装/亲子装',
                    '童鞋/婴儿鞋/亲子鞋',
                    '奢侈品',
                    '女鞋',
                    '箱包',
                    '男鞋',
                    '运动鞋',
                    '运动服/休闲服装',
                    '运动/瑜伽/健身/球迷用品',
                    '户外/登山/野营/旅行用品',
                    '运动包/户外包/配件',
                    '钟表类',
                    '时尚饰品',
                    '打火机/瑞士军刀/眼镜',
                    '户外鞋服'
                ) THEN 'G1服饰'
                WHEN main_firstcate IN (
                    '二手百货',
                    '肉蛋低温制品',
                    '水果蔬菜',
                    '海鲜水产及制品',
                    '零食/坚果/特产',
                    '咖啡/麦片/冲饮',
                    '食品卡券',
                    '茶',
                    '粮油米面/南北干货/调味品',
                    '农机/农具/农膜',
                    '农用物资',
                    '畜牧/养殖物资',
                    '鲜花速递/花卉仿真/绿植园艺',
                    '餐饮具',
                    '床上用品',
                    '家庭/个人清洁工具',
                    '居家布艺',
                    '家居饰品',
                    '居家日用',
                    '厨房/烹饪用具',
                    '收纳整理'
                ) THEN 'G7日用食杂'
                WHEN main_firstcate IN (
                    '医疗及健康服务',
                    '二手奢侈品',
                    '成人用品/情趣用品',
                    '康养用品',
                    '计生用品',
                    '药品',
                    '医疗器械',
                    '特医食品',
                    '手工艺/非遗技艺',
                    '黄金/彩宝/钻石/珍珠',
                    '文物商店/拍卖行',
                    '珠宝/文玩/收藏品（定制专用）',
                    '文玩/木作/书画/钱币',
                    '陶瓷/紫砂/建盏/茶周边',
                    '翡翠/和田玉/琥珀蜜蜡/其他玉石'
                ) THEN 'G8珠宝文玩与医疗健康'
                ELSE '其他'
            END AS cate_team
    FROM    (
                SELECT  A.shop_id,
                        A.first_cate AS main_firstcate,
                        B.second_cate AS main_second_cate
                FROM    main_firstcate AS A
                LEFT JOIN
                        main_secondcate AS B
                ON      A.shop_id=B.shop_id
            )
),
-- 底线种子违规信息
seed_penliaze_data AS (
    SELECT
        shop_id,
        rule_id,
        penalize_id,
        penalize_time,
        penalize_product_id,
        penalize_reason
    FROM
        (
            SELECT  A.shop_id,
                    A.rule_id,
                    A.penalize_id,
                    A.penalize_time,
                    case
                        -- 假货-商品罚单
                        when A.rule_id in (208647,224000,208722,208717,208715,208720,220230,208648,222765,219490,208716,208651,208696,208718,219489,208649,208650) then A.business_id
                        -- 假货-商家罚单
                        when A.rule_id in (216941,216949,219466,219472,220715,220713,219468,219469,219470,219471) then B1.product_id
                        -- 假货-订单罚单
                        when A.rule_id in (221454,220086) then B1.product_id
                        -- 假货-未知罚单
                        when A.rule_id in (212450,219467,220714,208719,208721) and object_type = 2 then A.business_id
                        when A.rule_id in (212450,219467,220714,208719,208721) and object_type != 2 then B1.product_id
                        -- 劣质-商品罚单
                        when A.rule_id in (223215,223216,223225,223226,223228,222671,222673,222678,222977,220733) then A.business_id
                        -- 劣质-商家罚单
                        when A.rule_id in (1912,1916,1914) then B2.product_id
                        -- 劣质-未知罚单
                        when A.rule_id in (223217,222676) and object_type = 2 then A.business_id
                        when A.rule_id in (223217,222676) and object_type != 2 then B2.product_id
                        -- 虚假宣传-商品罚单
                        when A.rule_id in (224287,223626,223290,223625,222141,222166,222178,223291,225524,223292,210005) then A.business_id
                        else -1
                    end as penalize_product_id,
                    penalize_reason
            FROM    (
                        SELECT  A.shop_id,
                                A.rule_id,
                                A.ticket_id as penalize_id,
                                A.ticket_create_time as penalize_time,
                                A.object_id as business_id,
                                A.violation_detail as penalize_reason,
                                A.object_type
                        FROM    ecom.dwd_gvn_penalty_ticket_info_df as A
                        left join shop_main_cate as B on A.shop_id = B.shop_id
                        WHERE   A.date='${date}'
                        AND     A.status=1
                        AND     A.revoke_way NOT IN (1, 2, 3)
                        AND     A.ticket_create_time >= '${DATE-59}'
                        AND     A.ticket_create_time < '${DATE+1}'
                        AND     (
                                (A.rule_id IN (
                                    -- 假货
                                    216941,216949,219466,220713,219472,208647,224000,208722,208717,208715,208720,220230,208648,222765,219490,208716,208651,208696,219489,208718,220715,212450,219467,219468,219469,219470,219471,208649,208650,220714,208719,208721,221454,220086,
                                    -- 劣质
                                    223215,223216,223217,223225,223226,223228,222671,222673,222676,222678,222977,220733,1912,1916,1914,
                                    -- 虚假宣传
                                    224287,223626,223290,223625,222141,222166,222178,223291,225524,223292,210005
                                ) and nvl(B.cate_team, '其他') not in ('G7日用食杂', 'G2快消跨境'))
                                or
                                (A.rule_id IN (
                                    -- 假货
                                    216941,216949,219466,220713,219472,208647,224000,208722,208717,208715,208720,220230,208648,222765,219490,208716,208651,208696,219489,208718,220715,212450,219467,219468,219469,219470,219471,208649,208650,220714,208719,208721,221454,220086,
                                    -- 劣质
                                    1916,223215,223216,223217,223225,223226,223228,222671,222673,222676,222678,222977,220733,
                                    -- 虚假宣传
                                    224287,223626,223290,223625,222141,222166,222178,223291,225524,223292,210005
                                ) and nvl(B.cate_team, '其他') in ('G7日用食杂', 'G2快消跨境'))
                                )
                        AND     A.ticket_type_code = 3
                        AND     A.is_deleted = 0
                    ) AS A
            LEFT JOIN ecom_govern_community.groupaudit_shop_seed_penalize_product as B1 on A.shop_id = B1.shop_id and A.penalize_id = B1.penalize_id and B1.date = '${date}' and B1.risk_type = 'jh' and A.rule_id in (216941,216949,219466,219472,220715,220713,219468,219469,219470,219471,212450,219467,220714,208719,208721,221454,220086)
            LEFT join ecom_govern_community.groupaudit_shop_seed_penalize_product as B2 on A.shop_id = B2.shop_id and A.penalize_id = B2.penalize_id and B2.date = '${date}' and B2.risk_type = 'lz' and A.rule_id in (1912,1916,1914,223217,222676)
        )
    WHERE   penalize_product_id > 0
)
INSERT OVERWRITE TABLE ecom_govern_community.groupaudit_shop_recallstrategy_v1_origin_groupshopinfo PARTITION (date = '${date}')
SELECT
    A.group_type,
    A.group_id,
    A.group_size,
    A.shop_id,
    A.shop_type_code,
    A.operate_status_code,
    A.stop_status_code,
    A.shop_open_time,

    case when B1.shop_id is not null then 1 else 0 end as is_jh_seed,
    B1.seed_penalize_info as jh_seed_penalize_info,
    B1.first_seed_penalize_time as jh_first_seed_penalize_time,
    case when B2.shop_id is not null then 1 else 0 end as is_lz_seed,
    B2.seed_penalize_info as lz_seed_penalize_info,
    B2.first_seed_penalize_time as lz_first_seed_penalize_time,
    case when B3.shop_id is not null then 1 else 0 end as is_xjxc_seed,
    B3.seed_penalize_info as xjxc_seed_penalize_info,
    B3.first_seed_penalize_time as xjxc_first_seed_penalize_time,

    C.product_cnt,
    C.p60d_update_product_cnt,
    C.latest_product_update_time,
    C.p60d_create_product_cnt,
    C.latest_product_create_time,

    nvl(D.p60d_pay_order_cnt, 0) as p60d_pay_order_cnt,
    nvl(D.p30d_pay_order_cnt, 0) as p30d_pay_order_cnt,

    E.group_id as strong_group_id
FROM
    group_base_data as A

    left join (
        SELECT
            shop_id,
            collect_list(cast(to_json(map(
                'rule_id',rule_id,
                'penalize_id',penalize_id,
                'penalize_time',penalize_time,
                'penalize_product_id',penalize_product_id,
                'penalize_reason', penalize_reason
            )) as string)) as seed_penalize_info,
            min(penalize_time) as first_seed_penalize_time
        FROM
            seed_penliaze_data
        WHERE
            rule_id in (216941,216949,219466,220713,219472,208647,224000,208722,208717,208715,208720,220230,208648,222765,219490,208716,208651,208696,219489,208718,220715,212450,219467,219468,219469,219470,219471,208649,208650,220714,208719,208721,221454,220086)
        GROUP BY
            shop_id
    ) as B1 on A.shop_id = B1.shop_id
    left join (
        SELECT
            shop_id,
            collect_list(cast(to_json(map(
                'rule_id',rule_id,
                'penalize_id',penalize_id,
                'penalize_time',penalize_time,
                'penalize_product_id',penalize_product_id,
                'penalize_reason', penalize_reason
            )) as string)) as seed_penalize_info,
            min(penalize_time) as first_seed_penalize_time
        FROM
            seed_penliaze_data
        WHERE
            rule_id in (223215,223216,223217,223225,223226,223228,222671,222673,222676,222678,222977,220733,1912,1916,1914)
        GROUP BY
            shop_id
    ) as B2 on A.shop_id = B2.shop_id
    left join (
        SELECT
            shop_id,
            collect_list(cast(to_json(map(
                'rule_id',rule_id,
                'penalize_id',penalize_id,
                'penalize_time',penalize_time,
                'penalize_product_id',penalize_product_id,
                'penalize_reason', penalize_reason
            )) as string)) as seed_penalize_info,
            min(penalize_time) as first_seed_penalize_time
        FROM
            seed_penliaze_data
        WHERE
            rule_id in (224287,223626,223290,223625,222141,222166,222178,223291,225524,223292,210005)
        GROUP BY
            shop_id
    ) as B3 on A.shop_id = B3.shop_id

    left join (
        select
            shop_id,
            count(prod_id) AS product_cnt,
            count(
                CASE WHEN update_time >= '${DATE-59}' AND update_time <= '${DATE+1}' THEN prod_id
                        ELSE NULL
                END
            ) AS p60d_update_product_cnt,
            MAX(update_time) AS latest_product_update_time,
            count(
                CASE WHEN create_time >= '${DATE-59}' AND create_time <= '${DATE+1}' THEN prod_id
                        ELSE NULL
                END
            ) AS p60d_create_product_cnt,
            MAX(create_time) AS latest_product_create_time
        FROM
            ecom.dim_prd_product_base_df
        WHERE
            date = '${date}'
        group by
            shop_id
    ) as C on A.shop_id = C.shop_id

    left join (
        SELECT  shop_id,
                count(1) AS p60d_pay_order_cnt,
                count(case when order_time >= '${DATE-29}' and order_time <= '${DATE+1}' then 1 else null end) as p30d_pay_order_cnt
        FROM    dm_temai.shop_order_order_data
        WHERE   date = '${date}'
        AND     order_time >= '${DATE-59}'
        AND     order_time <= '${DATE+1}'
        GROUP BY
                shop_id
    ) as D on A.shop_id = D.shop_id

    left join dm_temai.shop_same_people_shops_group_hard_lab as E on A.shop_id = E.shop_id and E.date = '${date}'