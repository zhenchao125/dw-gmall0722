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

insert into table ads_goods_count
select
    '$do_date',
    goodsid,
    mid_id,
    ct
from(
    select
        *,
        rank() over(partition by goodsid order by ct desc ) rk
    from(
        select
            goodsid,
            mid_id,
            count(*) ct
        from dws_user_action_wide_log
        where dt<='$do_date' and display_count>0
        group by goodsid, mid_id
    ) t1
)t2
where rk<=3;
"

$hive -e "$sql"


