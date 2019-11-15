#!/bin/bash
APP=gmall
hive=/opt/module/hive-1.2.1/bin/hive
hadoop=/opt/module/hadoop-2.7.2/bin/hadoop
# 默认到昨天, 还可以指定哪一天

if [[ -n $1 ]]; then
    do_date=$1
else
    do_date=`date -d '-1 day' +%F`
fi
sql="
use $APP;
insert overwrite table "$APP".dwd_display_log
PARTITION (dt='$do_date')
select 
	mid_id,
	user_id,
	version_code,
	version_name,
	lang,
	source,
	os,
	area,
	model,
	brand,
	sdk_version,
	gmail,
	height_width,
	app_time,
	network,
	lng,
	lat,
	get_json_object(event_json,'$.action') action,
	get_json_object(event_json,'$.goodsid') goodsid,
	get_json_object(event_json,'$.place') place,
	get_json_object(event_json,'$.extend1') extend1,
	get_json_object(event_json,'$.category') category,
	server_time
from "$APP".dwd_base_event_log 
where dt='$do_date' and event_name='display';


insert overwrite table "$APP".dwd_newsdetail_log
PARTITION (dt='$do_date')
select 
	mid_id,
	user_id,
	version_code,
	version_name,
	lang,
	source,
	os,
	area,
	model,
	brand,
	sdk_version,
	gmail,
	height_width,
	app_time,
	network,
	lng,
	lat,
	get_json_object(event_json,'$.entry') entry,
	get_json_object(event_json,'$.action') action,
	get_json_object(event_json,'$.goodsid') goodsid,
	get_json_object(event_json,'$.showtype') showtype,
	get_json_object(event_json,'$.news_staytime') news_staytime,
	get_json_object(event_json,'$.loading_time') loading_time,
	get_json_object(event_json,'$.type1') type1,
	get_json_object(event_json,'$.category') category,
	server_time
from "$APP".dwd_base_event_log 
where dt='$do_date' and event_name='newsdetail';


insert overwrite table "$APP".dwd_loading_log
PARTITION (dt='$do_date')
select 
	mid_id,
	user_id,
	version_code,
	version_name,
	lang,
	source,
	os,
	area,
	model,
	brand,
	sdk_version,
	gmail,
	height_width,
	app_time,
	network,
	lng,
	lat,
	get_json_object(event_json,'$.action') action,
	get_json_object(event_json,'$.loading_time') loading_time,
	get_json_object(event_json,'$.loading_way') loading_way,
	get_json_object(event_json,'$.extend1') extend1,
	get_json_object(event_json,'$.extend2') extend2,
	get_json_object(event_json,'$.type') type,
	get_json_object(event_json,'$.type1') type1,
	server_time
from "$APP".dwd_base_event_log 
where dt='$do_date' and event_name='loading';


insert overwrite table "$APP".dwd_ad_log
PARTITION (dt='$do_date')
select 
	mid_id,
	user_id,
	version_code,
	version_name,
	lang,
	source,
	os,
	area,
	model,
	brand,
	sdk_version,
	gmail,
	height_width,
	app_time,
	network,
	lng,
	lat,
	get_json_object(event_json,'$.entry') entry,
	get_json_object(event_json,'$.action') action,
	get_json_object(event_json,'$.content') content,
	get_json_object(event_json,'$.detail') detail,
	get_json_object(event_json,'$.source') ad_source,
	get_json_object(event_json,'$.behavior') behavior,
	get_json_object(event_json,'$.newstype') newstype,
	get_json_object(event_json,'$.show_style') show_style,
	server_time
from "$APP".dwd_base_event_log 
where dt='$do_date' and event_name='ad';


insert overwrite table "$APP".dwd_notification_log
PARTITION (dt='$do_date')
select 
	mid_id,
	user_id,
	version_code,
	version_name,
	lang,
	source,
	os,
	area,
	model,
	brand,
	sdk_version,
	gmail,
	height_width,
	app_time,
	network,
	lng,
	lat,
	get_json_object(event_json,'$.action') action,
	get_json_object(event_json,'$.noti_type') noti_type,
	get_json_object(event_json,'$.ap_time') ap_time,
	get_json_object(event_json,'$.content') content,
	server_time
from "$APP".dwd_base_event_log 
where dt='$do_date' and event_name='notification';


insert overwrite table "$APP".dwd_active_foreground_log
PARTITION (dt='$do_date')
select 
	mid_id,
	user_id,
	version_code,
	version_name,
	lang,
	source,
	os,
	area,
	model,
	brand,
	sdk_version,
	gmail,
	height_width,
	app_time,
	network,
	lng,
	lat,
get_json_object(event_json,'$.push_id') push_id,
get_json_object(event_json,'$.access') access,
	server_time
from "$APP".dwd_base_event_log 
where dt='$do_date' and event_name='active_foreground';


insert overwrite table "$APP".dwd_active_background_log
PARTITION (dt='$do_date')
select 
	mid_id,
	user_id,
	version_code,
	version_name,
	lang,
	source,
	os,
	area,
	model,
	brand,
	sdk_version,
	gmail,
	height_width,
	app_time,
	network,
	lng,
	lat,
	get_json_object(event_json,'$.active_source') active_source,
	server_time
from "$APP".dwd_base_event_log 
where dt='$do_date' and event_name='active_background';


insert overwrite table "$APP".dwd_comment_log
PARTITION (dt='$do_date')
select 
	mid_id,
	user_id,
	version_code,
	version_name,
	lang,
	source,
	os,
	area,
	model,
	brand,
	sdk_version,
	gmail,
	height_width,
	app_time,
	network,
	lng,
	lat,
	get_json_object(event_json,'$.comment_id') comment_id,
	get_json_object(event_json,'$.userid') userid,
	get_json_object(event_json,'$.p_comment_id') p_comment_id,
	get_json_object(event_json,'$.content') content,
	get_json_object(event_json,'$.addtime') addtime,
	get_json_object(event_json,'$.other_id') other_id,
	get_json_object(event_json,'$.praise_count') praise_count,
	get_json_object(event_json,'$.reply_count') reply_count,
	server_time
from "$APP".dwd_base_event_log 
where dt='$do_date' and event_name='comment';


insert overwrite table "$APP".dwd_favorites_log
PARTITION (dt='$do_date')
select 
	mid_id,
	user_id,
	version_code,
	version_name,
	lang,
	source,
	os,
	area,
	model,
	brand,
	sdk_version,
	gmail,
	height_width,
	app_time,
	network,
	lng,
	lat,
	get_json_object(event_json,'$.id') id,
	get_json_object(event_json,'$.course_id') course_id,
	get_json_object(event_json,'$.userid') userid,
	get_json_object(event_json,'$.add_time') add_time,
	server_time
from "$APP".dwd_base_event_log 
where dt='$do_date' and event_name='favorites';


insert overwrite table "$APP".dwd_praise_log
PARTITION (dt='$do_date')
select 
	mid_id,
	user_id,
	version_code,
	version_name,
	lang,
	source,
	os,
	area,
	model,
	brand,
	sdk_version,
	gmail,
	height_width,
	app_time,
	network,
	lng,
	lat,
	get_json_object(event_json,'$.id') id,
	get_json_object(event_json,'$.userid') userid,
	get_json_object(event_json,'$.target_id') target_id,
	get_json_object(event_json,'$.type') type,
	get_json_object(event_json,'$.add_time') add_time,
	server_time
from "$APP".dwd_base_event_log 
where dt='$do_date' and event_name='praise';


insert overwrite table "$APP".dwd_error_log
PARTITION (dt='$do_date')
select 
	mid_id,
	user_id,
	version_code,
	version_name,
	lang,
	source,
	os,
	area,
	model,
	brand,
	sdk_version,
	gmail,
	height_width,
	app_time,
	network,
	lng,
	lat,
	get_json_object(event_json,'$.errorBrief') errorBrief,
	get_json_object(event_json,'$.errorDetail') errorDetail,
	server_time
from "$APP".dwd_base_event_log 
where dt='$do_date' and event_name='error';
"

$hive -e "$sql"
