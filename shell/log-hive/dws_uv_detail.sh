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
insert overwrite table dws_uv_detail_day
partition(dt='$do_date')
select
  mid_id,
  concat_ws('|', collect_set(user_id)) user_id,
  concat_ws('|', collect_set(version_code)) version_code,
  concat_ws('|', collect_set(version_name)) version_name,
  concat_ws('|', collect_set(lang))lang,
  concat_ws('|', collect_set(source)) source,
  concat_ws('|', collect_set(os)) os,
  concat_ws('|', collect_set(area)) area,
  concat_ws('|', collect_set(model)) model,
  concat_ws('|', collect_set(brand)) brand,
  concat_ws('|', collect_set(sdk_version)) sdk_version,
  concat_ws('|', collect_set(gmail)) gmail,
  concat_ws('|', collect_set(height_width)) height_width,
  concat_ws('|', collect_set(app_time)) app_time,
  concat_ws('|', collect_set(network)) network,
  concat_ws('|', collect_set(lng)) lng,
  concat_ws('|', collect_set(lat)) lat
from dwd_start_log
where dt='$do_date'
group by mid_id;

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_uv_detail_wk
partition(wk_dt) --  2019-11-11_2019-11-17
select
  mid_id,
  concat_ws('|', collect_set(user_id)) user_id,
  concat_ws('|', collect_set(version_code)) version_code,
  concat_ws('|', collect_set(version_name)) version_name,
  concat_ws('|', collect_set(lang))lang,
  concat_ws('|', collect_set(source)) source,
  concat_ws('|', collect_set(os)) os,
  concat_ws('|', collect_set(area)) area,
  concat_ws('|', collect_set(model)) model,
  concat_ws('|', collect_set(brand)) brand,
  concat_ws('|', collect_set(sdk_version)) sdk_version,
  concat_ws('|', collect_set(gmail)) gmail,
  concat_ws('|', collect_set(height_width)) height_width,
  concat_ws('|', collect_set(app_time)) app_time,
  concat_ws('|', collect_set(network)) network,
  concat_ws('|', collect_set(lng)) lng,
  concat_ws('|', collect_set(lat)) lat,
  date_add(next_day('$do_date', 'mo'), -7),
  date_add(next_day('$do_date', 'mo'), -1),
  concat(date_add(next_day('$do_date', 'mo'), -7), '_', date_add(next_day('$do_date', 'mo'), -1))
from dws_uv_detail_day
where dt>=date_add(next_day('$do_date', 'mo'), -7) and dt<=date_add(next_day('$do_date', 'mo'), -1)
group by mid_id;


insert overwrite table dws_uv_detail_mn
partition(mn)
select
    mid_id,
    concat_ws('|', collect_set(user_id)) user_id,
    concat_ws('|', collect_set(version_code)) version_code,
    concat_ws('|', collect_set(version_name)) version_name,
    concat_ws('|', collect_set(lang)) lang,
    concat_ws('|', collect_set(source)) source,
    concat_ws('|', collect_set(os)) os,
    concat_ws('|', collect_set(area)) area,
    concat_ws('|', collect_set(model)) model,
    concat_ws('|', collect_set(brand)) brand,
    concat_ws('|', collect_set(sdk_version)) sdk_version,
    concat_ws('|', collect_set(gmail)) gmail,
    concat_ws('|', collect_set(height_width)) height_width,
    concat_ws('|', collect_set(app_time)) app_time,
    concat_ws('|', collect_set(network)) network,
    concat_ws('|', collect_set(lng)) lng,
    concat_ws('|', collect_set(lat)) lat,
    date_format('$do_date','yyyy-MM')
from dws_uv_detail_day
where date_format('$do_date', 'yyyy-MM')=date_format(dt, 'yyyy-MM')
group by mid_id;
"

$hive -e "$sql"


