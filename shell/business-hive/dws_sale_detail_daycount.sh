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
tmp_detail as
(
    select
        user_id,
        sku_id,
        sum(sku_num)  sku_num,
        count(*) order_count,
        sum(order_price * sku_num) order_amount
    from dwd_order_detail
    where dt='$do_date'
    group by user_id, sku_id
)

insert overwrite table dws_sale_detail_daycount partition(dt='$do_date')
select
    t1.user_id,
    t1.sku_id,
    u.gender,
    months_between('$do_date', u.birthday)/12  age,
    u.user_level,
    price,
    sku_name,
    tm_id,
    category3_id,
    category2_id,
    category1_id,
    category3_name,
    category2_name,
    category1_name,
    spu_id,
    t1.sku_num,
    t1.order_count,
    t1.order_amount
from(
    select * from tmp_detail
)t1
join dwd_user_info u
on t1.user_id=u.id and u.dt='$do_date'
join dwd_sku_info sku
on t1.sku_id=sku.id and sku.dt='$do_date';
"

$hive -e "$sql"


