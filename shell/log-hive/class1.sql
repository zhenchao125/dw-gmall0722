/*
日志行为宽表
 */
use gmall

insert overwrite table dws_user_action_wide_log
partition(dt='2019-11-13')
select
    mid_id,
    goodsid,
    sum(c1),
    sum(c2),
    sum(c3)
from(
    select
        mid_id,
        goodsid,
        count(*) c1,
        0 c2,
        0 c3
    from dwd_display_log
    where dt='2019-11-13'
    group by mid_id, goodsid
    union all
    select
        mid_id,
        target_id goodsid,
        0 c1,
        count(*) c2,
        0 c3
    from gmall.dwd_praise_log
    where dt='2019-11-13'
    group by mid_id, target_id
    union all
    select
        mid_id,
        course_id goodsid,
        0 c1,
        0 c2,
        count(*) c3
    from gmall.dwd_favorites_log
    where dt='2019-11-13'
    group by mid_id, course_id
)tem
group by mid_id, goodsid


/*新收藏用户
1. 先找到当天的新收藏用户


2. 进行count
*/
-- t1


select
    count(*)
from(
    select
        mid_id
    from gmall.dws_user_action_wide_log
    where dt<='2019-11-13' and favorite_count>0
    group by mid_id
    having min(dt)='2019-11-13'
)t1

/*
各个商品点击次数top3的用户
 */

insert into table ads_goods_count
select
    goodsid,
    mid_id,
    ct
from(
    select
        *,
        rank() over(partition by goodsid order by ct desc ) rk
    from(
        select
            goodsid,
            mid_id,
            count(*) ct
        from dws_user_action_wide_log
        where dt<='2019-11-13' and display_count>0
        group by goodsid, mid_id
    ) t1
)t2
where rk<=3


/*统计每日各类别下点击次数top10的商品*/

-- t1
select
    category,
    goodsid,
    count(*) ct
from dwd_display_log
where dt='2019-11-13' and action=2
group by category, goodsid

-- t2
select
    *,
    rank() over(partition by category order by ct desc ) rk
from t1;

select
    '2019-11-13',
    category,
    goodsid,
    ct
from (
    select
        *,
        rank() over(partition by category order by ct desc ) rk
    from (
        select
            category,
            goodsid,
            count(*) ct
        from dwd_display_log
        where dt='2019-11-13' and action=2
        group by category, goodsid
    )t1

)t2
where rk<=10

/*
点击次数最多的10个用户点击的商品次数top10

1. 先找到点击次数最多的10个用户
2. 找到10个用户点击过的商品
3. 按照商品分组, 降序, limit10

 */

-- t1
select
    mid_id,
    count(*) ct
from dws_user_action_wide_log
where dt<='2019-11-13' and display_count>0
group by mid_id
order by ct desc
limit 10

-- t2  计算出来了前10的每个用户对每个商品的点击次数
select
    dl.mid_id,
    ct,
    dl.goodsid,
    count(*) ct1
from dwd_display_log dl
join t1
on dl.mid_id=t1.mid_id
group dl.mid_id, ct, dl.goodsid

-- t3
select
    *,
    rank() over(partition by mid_id order by ct1 desc) rk
from t2

select
    '2019-11-13',
    mid_id,
    ct,
    goodsid,
    ct1
from (
    select
        *,
        rank() over(partition by mid_id order by ct1 desc) rk
    from (
        select
            dl.mid_id,
            ct,
            dl.goodsid,
            count(*) ct1
        from dwd_display_log dl
        join (
            select
                mid_id,
                count(*) ct
            from dws_user_action_wide_log
            where dt<='2019-11-13' and display_count>0
            group by mid_id
            order by ct desc
            limit 10
        )t1
        on dl.mid_id=t1.mid_id
        group by dl.mid_id, ct, dl.goodsid
    )t2
)t3
where rk<=10

/*月活跃率

1. 月活
2. 新增表

*/

select
    '2019-11-13',
    date_format('2019-11-13', 'yyyy-MM'),
    ct1 / ct2 * 100
from (
    select count(*) ct1 from dws_uv_detail_mn where mn=date_format('2019-11-13', 'yyyy-MM')
) dw
join (
    select count(*)ct2  from dws_new_mid_day
) md

