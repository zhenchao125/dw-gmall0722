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
t1 as(
    select
        count(*) c1,
        0 c2
    from dws_uv_detail_day
    where dt='$do_date'
),
t2 as(
    select
        0 c1,
        count(*) c2
    from dws_new_mid_day
    where create_date='$do_date'
)

insert into table ads_user_convert_day
select
    '$do_date',
    sum(c1),
    sum(c2),
    sum(c2) / sum(c1) * 100
from(
    select * from t1
    union all
    select * from t2
)t3;
"

$hive -e "$sql"


