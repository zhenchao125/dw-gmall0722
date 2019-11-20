#!/bin/bash
hive=/opt/module/hive-1.2.1/bin/hive
hadoop=/opt/module/hadoop-2.7.2/bin/hadoop
# 默认到昨天, 还可以指定哪一天

if [[ -n $1 ]]; then
    do_date=$1
else
    do_date=`date -d '-1 day' +%F`
fi

sql="
use gmall;

with
tmp_count as(
    select  -- 统计每个用户, 对每个品牌, 下的一级品的下单次数
        user_id,
        sku_tm_id,
        sku_category1_id,
        max(sku_category1_name) sku_category1_name,
        sum(order_count) sum_order_count
    from dws_sale_detail_daycount
    where date_format('$do_date', 'yyyy-MM')=date_format(dt, 'yyyy-MM')
    group by user_id, sku_tm_id, sku_category1_id
)
insert into table ads_sale_tm_category1_stat_mn
select
    sku_tm_id,
    sku_category1_id,
    max(sku_category1_name),
    sum(if(sum_order_count > 0, 1, 0)),  -- 购买过商品的人数
    sum(if(sum_order_count > 1, 1, 0)),  -- 购买过2次及以上商品的人数
    sum(if(sum_order_count > 1, 1, 0)) / sum(if(sum_order_count > 0, 1, 0)) * 100,
    sum(if(sum_order_count > 2, 1, 0)),   -- 购买过3次及以上商品的人数
    sum(if(sum_order_count > 2, 1, 0)) / sum(if(sum_order_count > 0, 1, 0)) * 100,
    date_format('$do_date', 'yyyy-MM'),
    '$do_date'
from tmp_count
group by sku_tm_id, sku_category1_id
"

$hive -e "$sql"


