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
insert into table dws_new_mid_day
select
    ud.mid_id,
    ud.user_id ,
    ud.version_code ,
    ud.version_name ,
    ud.lang ,
    ud.source,
    ud.os,
    ud.area,
    ud.model,
    ud.brand,
    ud.sdk_version,
    ud.gmail,
    ud.height_width,
    ud.app_time,
    ud.network,
    ud.lng,
    ud.lat,
    '$do_date'
from dws_uv_detail_day ud
left join dws_new_mid_day nmd
on ud.mid_id=nmd.mid_id
where nmd.mid_id is null and ud.dt='$do_date';


insert into table ads_new_mid_count
select
  '$do_date',
  count(*)
from dws_new_mid_day
where create_date='$do_date';
"

$hive -e "$sql"


