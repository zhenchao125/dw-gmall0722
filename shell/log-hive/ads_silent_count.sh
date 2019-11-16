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
insert into table ads_silent_count
select
  '$do_date',
  count(*)
from(
  select
    mid_id
  from dws_uv_detail_day
  group by mid_id
  having count(*)=1 and max(dt)<=date_add('$do_date', -7)
) temp;
"

$hive -e "$sql"


