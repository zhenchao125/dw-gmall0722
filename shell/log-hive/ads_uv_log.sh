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
insert into table ads_uv_count
select
  dd.dt,
  dd.ct,
  dw.ct,
  dm.ct,
  if('$do_date'=date_add(next_day('$do_date', 'mo'), -1)  , 'Y',  'N'),
  if('$do_date'=last_day('$do_date')  , 'Y',  'N')
from
(
  select
    '$do_date' dt,
    count(*) ct
  from dws_uv_detail_day
  where dt='$do_date'
) dd join(
select
  '$do_date' dt,
    count(*) ct
  from dws_uv_detail_wk
  where wk_dt=concat(date_add(next_day('$do_date', 'mo'), -7), '_', date_add(next_day('$do_date', 'mo'), -1))
) dw on dd.dt=dw.dt join(
  select
    '$do_date' dt,
    count(*) ct
  from dws_uv_detail_mn
  where mn=date_format('$do_date', 'yyyy-MM')
) dm on dd.dt=dm.dt;
"

$hive -e "$sql"


