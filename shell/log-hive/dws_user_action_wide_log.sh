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

insert overwrite table dws_user_action_wide_log
partition(dt='$do_date')
select
    mid_id,
    goodsid,
    sum(c1),
    sum(c2),
    sum(c3)
from(
    select
        mid_id,
        goodsid,
        count(*) c1,
        0 c2,
        0 c3
    from dwd_display_log
    where dt='$do_date'
    group by mid_id, goodsid
    union all
    select
        mid_id,
        target_id goodsid,
        0 c1,
        count(*) c2,
        0 c3
    from gmall.dwd_praise_log
    where dt='$do_date'
    group by mid_id, target_id
    union all
    select
        mid_id,
        course_id goodsid,
        0 c1,
        0 c2,
        count(*) c3
    from gmall.dwd_favorites_log
    where dt='$do_date'
    group by mid_id, course_id
)tem
group by mid_id, goodsid;
"

$hive -e "$sql"


