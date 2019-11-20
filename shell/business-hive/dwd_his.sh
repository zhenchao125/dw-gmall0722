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

insert overwrite table dwd_order_info_his_tmp
select  -- 开始: 把拉链表中没有更新的和需要更新的给保留下来
    t1.id,
    t1.total_amount,
    t1.order_status,
    t1.user_id,
    t1.payment_way,
    t1.out_trade_no,
    t1.create_time,
    t1.operate_time,
    t1.start_date,
    if(t2.id is null, t1.end_date, date_add('$do_date',-1))
from dwd_order_info_his t1
left join
(
    select
        id,
        total_amount,
        order_status,
        user_id,
        payment_way,
        out_trade_no,
        create_time,
        operate_time
    from dwd_order_info
    where dt='$do_date'
) t2
on t1.id=t2.id and t1.end_date='9999-99-99'

union all
select
    id,
    total_amount,
    order_status,
    user_id,
    payment_way,
    out_trade_no,
    create_time,
    operate_time,
    '$do_date',
    '9999-99-99'
from dwd_order_info
where dt='$do_date'
;



insert overwrite table dwd_order_info_his
select * from dwd_order_info_his_tmp;
"

$hive -e "$sql"


