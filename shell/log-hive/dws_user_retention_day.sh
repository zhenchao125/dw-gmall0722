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
insert overwrite table dws_user_retention_day
partition(dt='$do_date')
select  -- 3日留存
    nm.mid_id,
    nm.user_id,
    nm.version_code,
    nm.version_name,
    nm.lang,
    nm.source,
    nm.os,
    nm.area,
    nm.model,
    nm.brand,
    nm.sdk_version,
    nm.gmail,
    nm.height_width,
    nm.app_time,
    nm.network,
    nm.lng,
    nm.lat,
    nm.create_date,
    3 retention_day
from dws_new_mid_day  nm
join dws_uv_detail_day dd
on nm.mid_id=dd.mid_id
where dd.dt='$do_date' and nm.create_date=date_add('$do_date', -3)
union all
select  -- 2日留存
    nm.mid_id,
    nm.user_id,
    nm.version_code,
    nm.version_name,
    nm.lang,
    nm.source,
    nm.os,
    nm.area,
    nm.model,
    nm.brand,
    nm.sdk_version,
    nm.gmail,
    nm.height_width,
    nm.app_time,
    nm.network,
    nm.lng,
    nm.lat,
    nm.create_date,
    2 retention_day
from dws_new_mid_day  nm
join dws_uv_detail_day dd
on nm.mid_id=dd.mid_id
where dd.dt='$do_date' and nm.create_date=date_add('$do_date', -2)
union all
select  -- 3日留存
    nm.mid_id,
    nm.user_id,
    nm.version_code,
    nm.version_name,
    nm.lang,
    nm.source,
    nm.os,
    nm.area,
    nm.model,
    nm.brand,
    nm.sdk_version,
    nm.gmail,
    nm.height_width,
    nm.app_time,
    nm.network,
    nm.lng,
    nm.lat,
    nm.create_date,
    1 retention_day
from dws_new_mid_day  nm
join dws_uv_detail_day dd
on nm.mid_id=dd.mid_id
where dd.dt='$do_date' and nm.create_date=date_add('$do_date', -1);
"

$hive -e "$sql"


