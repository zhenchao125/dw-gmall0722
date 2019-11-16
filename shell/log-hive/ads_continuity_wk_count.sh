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
select
  '$do_date',
  concat(date_add(next_day('$do_date', 'mo'), -21), '_', date_add(next_day('$do_date', 'mo'), -1)),
  count(*)
from(
  select
    mid_id
  from dws_uv_detail_wk
  where wk_dt>=concat(date_add(next_day('$do_date', 'mo'), -21), '_', date_add(next_day('$do_date', 'mo'), -15))  and wk_dt<=concat(date_add(next_day('$do_date', 'mo'), -7), '_', date_add(next_day('$do_date', 'mo'), -1))
  group by mid_id
  having count(*)=3
)tmp;
"

$hive -e "$sql"


