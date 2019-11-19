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

insert into table ads_gmv_sum_day
select
    '$do_date',
    sum(order_count),
    sum(order_amount),
    sum(payment_amount)
from gmall.dws_user_action
where dt='$do_date'
;
"
$hive -e "$sql"


