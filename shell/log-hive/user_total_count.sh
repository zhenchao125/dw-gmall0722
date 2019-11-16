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
insert overwrite table dws_user_total_count_day
partition(dt='$do_date')
select
  mid_id,
  count(*)
from  dwd_start_log
where dt='$do_date'
group by mid_id;


insert overwrite table ads_user_total_count
partition(dt='$do_date')
select
  if(cd.mid_id is null, tc.mid_id, cd.mid_id),
  if(cd.subtotal is null , 0, cd.subtotal),
  if(tc.total is null, 0, tc.total) + if(cd.subtotal is null , 0, cd.subtotal)
from (
  select * from dws_user_total_count_day where dt='$do_date'
) cd
full join(
  select * from ads_user_total_count where dt=date_add('$do_date', -1)
) tc
on cd.mid_id=tc.mid_id;
"

$hive -e "$sql"


