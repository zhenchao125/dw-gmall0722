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

insert into table ads_goods_display_top10
select
    '$do_date',
    category,
    goodsid,
    ct
from (
    select
        *,
        rank() over(partition by category order by ct desc ) rk
    from (
        select
            category,
            goodsid,
            count(*) ct
        from dwd_display_log
        where dt='$do_date' and action=2
        group by category, goodsid
    )t1

)t2
where rk<=10
"

$hive -e "$sql"


