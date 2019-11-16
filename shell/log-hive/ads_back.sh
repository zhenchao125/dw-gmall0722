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

insert into table ads_back_count
select
    '$do_date',  -- 2019-11-19
    concat(date_add(next_day('$do_date', 'mo'), -7), '_', date_add(next_day('$do_date', 'mo'), -1)),
    count(*)
from(
    select
      current_wk_alive.mid_id
    from (
      select
        mid_id
      from dws_uv_detail_wk
      where wk_dt=concat(date_add(next_day('$do_date', 'mo'), -7), '_', date_add(next_day('$do_date', 'mo'), -1))
    ) current_wk_alive
    left join(
      select
        mid_id
      from dws_new_mid_day
      where create_date>=date_add(next_day('$do_date', 'mo'), -7) and create_date<=date_add(next_day('$do_date', 'mo'), -1)
    ) current_wk_new
    on current_wk_alive.mid_id=current_wk_new.mid_id
    left join(
      select
        mid_id
      from dws_uv_detail_wk
      where wk_dt=concat(date_add(next_day('$do_date', 'mo'), -14), '_', date_add(next_day('$do_date', 'mo'), -8))
    )last_wk_alive
    on current_wk_alive.mid_id=last_wk_alive.mid_id
    where current_wk_new.mid_id is null and last_wk_alive.mid_id is null
) tmp
;
"

$hive -e "$sql"


