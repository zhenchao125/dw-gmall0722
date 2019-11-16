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
insert into table ads_user_retention_day_count
select
  create_date,
  retention_day,
  count(*) retention_count
from dws_user_retention_day
where dt='$do_date'
group by create_date, retention_day
;

insert into table ads_user_retention_day_rate
select
  '$do_date',
  dc.create_date,
  dc.retention_day,
  dc.retention_count,
  mc.new_mid_count,
  dc.retention_count / mc.new_mid_count * 100
from ads_user_retention_day_count dc
join ads_new_mid_count mc
on dc.create_date=mc.create_date   --
where date_add(dc.create_date, retention_day)='$do_date'
;
"

$hive -e "$sql"


