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
insert into table ads_continuity_uv_count
select
  '$do_date',
  concat(date_add('$do_date', -6), '_', '$do_date'),
  count(*)
from(
  select
    mid_id
  from(
    select
      mid_id
    from(
      select
        mid_id,
        dt,
        rank()  over(partition by mid_id order by dt) rk
      from dws_uv_detail_day
      where dt>=date_add('$do_date', -6) and dt<='$do_date'
    )t1
    group by mid_id, date_add(dt, -rk)
    having count(*)>=3
  ) t2
  group by mid_id
)t3
;
"

$hive -e "$sql"


