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
insert into table ads_new_favorites_mid_day
select
    '$do_date',
    count(*)
from(
    select
        mid_id
    from dws_user_action_wide_log
    where dt<='$do_date' and favorite_count>0
    group by mid_id
    having min(dt)='$do_date'
)t1
"

$hive -e "$sql"


