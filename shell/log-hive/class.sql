# ods 数据导入
use gmall;
load data inpath '/origin_data/gmall/log/topic_start/2019-11-12' into table ods_start_log partition(dt='2019-11-12')
load data inpath '/origin_data/gmall/log/topic_event/2019-11-12' into table ods_event_log partition(dt='2019-11-12')




# dwd start 数据导入

use gmall;
insert overwrite table dwd_start_log partition(dt='2019-11-12')
select
  get_json_object(line,'$.mid') mid_id,
    get_json_object(line,'$.uid') user_id,
    get_json_object(line,'$.vc') version_code,
    get_json_object(line,'$.vn') version_name,
    get_json_object(line,'$.l') lang,
    get_json_object(line,'$.sr') source,
    get_json_object(line,'$.os') os,
    get_json_object(line,'$.ar') area,
    get_json_object(line,'$.md') model,
    get_json_object(line,'$.ba') brand,
    get_json_object(line,'$.sv') sdk_version,
    get_json_object(line,'$.g') gmail,
    get_json_object(line,'$.hw') height_width,
    get_json_object(line,'$.t') app_time,
    get_json_object(line,'$.nw') network,
    get_json_object(line,'$.ln') lng,
    get_json_object(line,'$.la') lat,
    get_json_object(line,'$.entry') entry,
    get_json_object(line,'$.open_ad_type') open_ad_type,
    get_json_object(line,'$.action') action,
    get_json_object(line,'$.loading_time') loading_time,
    get_json_object(line,'$.detail') detail,
    get_json_object(line,'$.extend1') extend1
from gmall.ods_start_log
where dt='2019-11-12'


# dwd 事件日志基础表导入
use gmall;

insert overwrite dwd_base_event_log
partition(dt='2019-11-12')
select
  base_analizer(line,'mid') as mid_id,
  base_analizer(line,'uid') as user_id,
  base_analizer(line,'vc') as version_code,
  base_analizer(line,'vn') as version_name,
  base_analizer(line,'l') as lang,
  base_analizer(line,'sr') as source,
  base_analizer(line,'os') as os,
  base_analizer(line,'ar') as area,
  base_analizer(line,'md') as model,
  base_analizer(line,'ba') as brand,
  base_analizer(line,'sv') as sdk_version,
  base_analizer(line,'g') as gmail,
  base_analizer(line,'hw') as height_width,
  base_analizer(line,'t') as app_time,
  base_analizer(line,'nw') as network,
  base_analizer(line,'ln') as lng,
  base_analizer(line,'la') as lat,
  event_name,
  event_json,
  base_analizer(line,'st') as server_time
from ods_event_log
lateral view flat_analizer(base_analizer(line, 'et')) tmp as event_name, event_json
where dt='2019-11-12'


# 向事件表导入数据

insert overwrite table dwd_display_log
partition(dt='2019-11-12')
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
from dwd_base_event_log
where dt='2019-11-12' and event_name='display';

/*
collect_set
collect_list

concat(a, "_", b)
concat_ws("_", set)
 */

/*
活跃主题
 */

-- 日活明细
insert overwrite table dws_uv_detail_day
partition(dt='2019-11-12')
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
where dt='2019-11-12'
group by mid_id


-- 周活明细

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
  date_add(next_day('2019-11-12', 'mo'), -7),
  date_add(next_day('2019-11-12', 'mo'), -1),
  concat(date_add(next_day('2019-11-12', 'mo'), -7), '_', date_add(next_day('2019-11-12', 'mo'), -1))
from dws_uv_detail_day
where dt>=date_add(next_day('2019-11-12', 'mo'), -7) and dt<=date_add(next_day('2019-11-12', 'mo'), -1)
group by mid_id


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
    date_format('2019-11-12','yyyy-MM')
from dws_uv_detail_day
where date_format('2019-11-12', 'yyyy-MM')=date_format(dt, 'yyyy-MM')
group by mid_id
;


# ads 活跃
insert into table ads_uv_count
select
  dd.dt,
  dd.ct,
  dw.ct,
  dm.ct,
  if('2019-11-12'=date_add(next_day('2019-11-12', 'mo'), -1)  , 'Y',  'N'),
  if('2019-11-12'=last_day('2019-11-12')  , 'Y',  'N')
from
(
  select
    '2019-11-12' dt,
    count(*) ct
  from dws_uv_detail_day
  where dt='2019-11-12'
) dd join(
select
  '2019-11-12' dt,
    count(*) ct
  from dws_uv_detail_wk
  where wk_dt=concat(date_add(next_day('2019-11-12', 'mo'), -7), '_', date_add(next_day('2019-11-12', 'mo'), -1))
) dw on dd.dt=dw.dt join(
  select
    '2019-11-12' dt,
    count(*) ct
  from dws_uv_detail_mn
  where mn=date_format('2019-11-12', 'yyyy-MM')
) dm on dd.dt=dm.dt

-- 新增主题
/*
dws
  存每天的新增设备的明细  数据来源日活
  思路:
    日活表中启动的有曾经启动, 也有今天第一次启动

    dws_new_mid  新增

    13日启动   dws_uv_detail_day left join dws_new_mid



ads

*/
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
    '2019-11-12'
from dws_uv_detail_day ud
left join dws_new_mid_day nmd
on ud.mid_id=nmd.mid_id
where nmd.mid_id is null and ud.dt='2019-11-12'

-- ads 新增
select
  '2019-11-12',
  count(*)
from dws_new_mid_day mmd
where mmd.create_date='2019-11-12'


-- 用户流程主题
/*
需求那些表的数据
   新增表
   日活表
  日期       新增

2019-11-12  1000
1日流存
  2019-11-13日的日活 + 2019-11-12  的新增   join 内连接
2日流存
  2019-11-14日的日活 + 019-11-12  的新增
3日流存
  2019-11-15日的日活 + 019-11-12  的新增

2019-11-13 日的任务
  12日 1日留存
  11日 2日留存
  10日 3日留存
 */


-- dws留存
 -- 每日留存明细
 -- 2019-12-15 计算这个天的数据
insert overwrite table dws_user_retention_day
partition(dt='2019-11-15')
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
where dd.dt='2019-11-15' and nm.create_date=date_add('2019-11-15', -3)
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
where dd.dt='2019-11-15' and nm.create_date=date_add('2019-11-15', -2)
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
where dd.dt='2019-11-15' and nm.create_date=date_add('2019-11-15', -1)

-- ads留存
-- 留存数 / 新增数

insert into table ads_user_retention_day_count
select
  create_date,
  retention_day,
  count(*) retention_count
from dws_user_retention_day
where dt='2019-11-15'
group by create_date, retention_day
;

insert into table ads_user_retention_day_rate

select
  '2019-11-15',
  dc.create_date,
  dc.retention_day,
  dc.retention_count,
  mc.new_mid_count,
  dc.retention_count / mc.new_mid_count * 100
from ads_user_retention_day_count dc
join ads_new_mid_count mc
on dc.create_date=mc.create_date   --
-- where date_add(dc.create_date, retention_day)='2019-11-15'
where dc.create_date='2019-11-12' and retention_day=3
;
/*union all
select
  '2019-11-15',
  dc.create_date,
  dc.retention_day,
  dc.retention_count,
  mc.new_mid_count,
  dc.retention_count / mc.new_mid_count * 100
from ads_user_retention_day_count dc
join ads_new_mid_count mc
on dc.create_date=mc.create_date   --
-- where date_add(dc.create_date, retention_day)='2019-11-15'
where dc.create_date='2019-11-13' and retention_day=2
;*/



-- 沉默用户
--
insert into table ads_silent_count
select
  '2019-11-25',
  count(*)
from(
  select
    mid_id
  from dws_uv_detail_day
  group by mid_id
  having count(*)=1 and max(dt)<=date_add('2019-11-25', -7)
) temp
;


-- 回流用户

-- 本周活跃-本周新增-上周活跃

--先算回流mid
use gmall;
select
  current_wk_alive.mid_id
from (
  select
    mid_id
  from dws_uv_detail_wk
  where wk_dt=concat(date_add(next_day('2019-11-19', 'mo'), -7), '_', date_add(next_day('2019-11-19', 'mo'), -1))
) current_wk_alive
left join(
  select
    mid_id
  from dws_new_mid_day
  where create_date>=date_add(next_day('2019-11-19', 'mo'), -7) and create_date<=date_add(next_day('2019-11-19', 'mo'), -1)
) current_wk_new
on current_wk_alive.mid_id=current_wk_new.mid_id
left join(
  select
    mid_id
  from dws_uv_detail_wk
  where wk_dt=concat(date_add(next_day('2019-11-19', 'mo'), -14), '_', date_add(next_day('2019-11-19', 'mo'), -8))
)last_wk_alive
on current_wk_alive.mid_id=last_wk_alive.mid_id
where current_wk_new.mid_id is null and last_wk_alive.mid_id is null
;


-- 流失用户

-- 数据 日活  用户最大的登录日期还在7天前

insert into table ads_wastage_count
select
  '2019-11-19',
  count(*)
from(
  select
    mid_id
  from dws_uv_detail_day
  group by mid_id
  having max(dt)<=date_add('2019-11-19', -7)
)temp



-- 连续3周活跃
select
  '2019-11-19',
  concat(date_add(next_day('2019-11-19', 'mo'), -21), '_', date_add(next_day('2019-11-19', 'mo'), -1)),
  count(*)
from(
  select
    mid_id
  from dws_uv_detail_wk
  where wk_dt>=concat(date_add(next_day('2019-11-19', 'mo'), -21), '_', date_add(next_day('2019-11-19', 'mo'), -15))  and wk_dt<=concat(date_add(next_day('2019-11-19', 'mo'), -7), '_', date_add(next_day('2019-11-19', 'mo'), -1))
  group by mid_id
  having count(*)=3
)tmp

-- 最近七天内连续三天活跃用户数
/*
  10日-16日
  mid   dt        rank   dt1
  1   2019-11-11   1     2019-11-10
  1   2019-11-12   2     2019-11-10
  1   2019-11-13   3     2019-11-10

  1   2019-11-15   4     2019-11-11
  1   2019-11-16   5     2019-11-11
  1   2019-11-17   6     2019-11-11

  2   2019-11-12   1
  2   2019-11-13   2
  2   2019-11-15   3

*/

insert into table ads_continuity_uv_count
select
  '2019-11-16',
  concat(date_add('2019-11-16', -6), "_", '2019-11-16'),
  count(*)
from(
  select
    mid_id
  from(
    select
      mid_id
    from(
      select
        mid_id,
        dt,
        rank()  over(partition by mid_id order by dt) rk
      from dws_uv_detail_day
      where dt>=date_add('2019-11-16', -6) and dt<='2019-11-16'
    )t1
    group by mid_id, date_add(dt, -rk)
    having count(*)>=3
  ) t2
  group by mid_id
)t3

-- 思路2

/*
mid   dt
  1   2019-11-11    2
  1   2019-11-12    3
  1   2019-11-13    3

  1   2019-11-15    2
  1   2019-11-16    11111
  1   2019-11-17    111111
----
  2   2019-11-12
  2   2019-11-13
  2   2019-11-15

 */

-- t1
select
  mid_id,
  dt,
  lead(dt, 2, '9999-99-99')  over(partition by mid_id order by dt) lead2
from dws_uv_detail_day
where dt>=date_add('2019-11-16', -6) and dt<='2019-11-16'

-- t2
select
  distinct(mid_id) mid_id
from t1
where datediff(lead2, dt)=2

select
  '2019-11-16',
  concat(date_add('2019-11-16', -6), "_", '2019-11-16'),
  count(*)
from(
  select
    distinct(mid_id) mid_id
  from (
    select
      mid_id,
      dt,
      lead(dt, 2, '9999-99-99') over(partition by mid_id order by dt) lead2
    from dws_uv_detail_day
    where dt>=date_add('2019-11-16', -6) and dt<='2019-11-16'
  )t1
  where datediff(lead2, dt)=2
) t2


-- 每个用户登录次数

--dws 每个用户每天登录次数的统计

insert overwrite table dws_user_total_count_day
partition(dt='2019-11-12')
select
  mid_id,
  count(*)
from  dwd_start_log
where dt='2019-11-12'
group by mid_id

--ads

/*
用户	日期			    小计	总计
mid1	2019-02-10		10		10
mid1	2019-02-11		12		22
mid2	2019-02-10		15		15
mid2	2019-02-11		12		27

dws_user_total_count_day
    mid1   10   2019-11-12
    mid2   20   2019-11-12


    mid1   100   2019-11-13
    mid3   300   2019-11-13



ads_user_total_count
  mid1 10 10 2019-11-12
  mid2 20 20 2019-11-12

  mid1 10 110 2019-11-13
  mid2 0  20 2019-11-13
  mid3 300 300 2019-11-13



2019-12-13

 */

insert overwrite table ads_user_total_count
partition(dt='2019-11-12')
select
  if(cd.mid_id is null, tc.mid_id, cd.mid_id),
  if(cd.subtotal is null , 0, cd.subtotal),
  if(tc.total is null, 0, tc.total) + if(cd.subtotal is null , 0, cd.subtotal)
from (
  select * from dws_user_total_count_day where cd.dt='2019-11-12'
) cd
full join(
  select * from ads_user_total_count where tc.dt=date_add('2019-11-12', -1);
) tc
on cd.mid_id=tc.mid_id





















