use gmall;

/*# gmv*/
select
    '2019-11-19',
    sum(order_count),
    sum(order_amount),
    sum(payment_amount)
from gmall.dws_user_action
where dt='2019-11-19'
;


/*用户新鲜度*/

with
t1 as(
    select
        count(*) c1,
        0 c2
    from dws_uv_detail_day
    where dt='2019-11-12'
),
t2 as(
    select
        0 c1,
        count(*) c2
    from dws_new_mid_day
    where create_date='2019-11-12'
)

insert into table ads_user_convert_day
select
    '2019-11-12',
    sum(c1),
    sum(c2),
    sum(c2) / sum(c1) * 100
from(
    select * from t1
    union all
    select * from t2
)t3

/*漏斗分析
需要的表:
    ads日活(周活, 月活)
    宽表
*/
select
    t1.dt,
    t2.day_count,
    c1,
    c1 / t2.day_count * 100,
    c2,
    c2 / c1 * 100
from(
    select
        '2019-11-19' dt,
        sum(if(order_count > 0, 1, 0)) c1,  -- 总的下单人数
        sum(if(payment_count >0, 1, 0))  c2 -- 总的支付人数
    from dws_user_action
    where dt='2019-11-19'
) t1 join ads_uv_count t2
on t1.dt=t2.dt


/* 品牌复购率 */

with
tmp_detail as
(
    select
        user_id,
        sku_id,
        sum(sku_num)  sku_num,
        count(*) order_count,
        sum(order_price * sku_num) order_amount
    from dwd_order_detail
    where dt='2019-11-19'
    group by user_id, sku_id
)

insert overwrite table dws_sale_detail_daycount partition(dt='2019-11-19')
select
    t1.user_id,
    t1.sku_id,
    u.gender,
    months_between('2019-02-10', u.birthday)/12  age,
    u.user_level,
    price,
    sku_name,
    tm_id,
    category3_id,
    category2_id,
    category1_id,
    category3_name,
    category2_name,
    category1_name,
    spu_id,
    t1.sku_num,
    t1.order_count,
    t1.order_amount
from(
    select * from tmp_detail
)t1
join dwd_user_info u
on t1.user_id=u.id and u.dt='2019-11-19'
join dwd_sku_info sku
on t1.sku_id=sku.id and sku.dt='2019-11-19'

/*每个品牌下每个一级品类的复购率

1.


*/

with
tmp_count as(
    select  -- 统计每个用户, 对每个品牌, 下的一级品的下单次数
        user_id,
        sku_tm_id,
        sku_category1_id,
        max(sku_category1_name) sku_category1_name,
        sum(order_count) sum_order_count
    from dws_sale_detail_daycount
    where date_format('2019-11-20', 'yyyy-MM')=date_format(dt, 'yyyy-MM')
    group by user_id, sku_tm_id, sku_category1_id
)

select
    sku_tm_id,
    sku_category1_id,
    max(sku_category1_name),
    sum(if(sum_order_count > 0, 1, 0)),  -- 购买过商品的人数
    sum(if(sum_order_count > 1, 1, 0)),  -- 购买过2次及以上商品的人数
    sum(if(sum_order_count > 1, 1, 0)) / sum(if(sum_order_count > 0, 1, 0)) * 100,
    sum(if(sum_order_count > 2, 1, 0)),   -- 购买过3次及以上商品的人数
    sum(if(sum_order_count > 2, 1, 0)) / sum(if(sum_order_count > 0, 1, 0)) * 100
from tmp_count
group by sku_tm_id, sku_category1_id



/*各等级用户对应的复购率前十的商品排行*/
use gmall;
with
tmp_count as(
    select
        user_level,
        user_id,
        sku_id,
        sum(order_count) sum_order_count
    from dws_sale_detail_daycount
    where date_format('2019-11-20', 'yyyy-MM')=date_format(dt, 'yyyy-MM')
    group by user_level, user_id, sku_id

)
select
    user_level, sku_id,
    c1,
    c2,
    c3
from(
    select
        user_level, sku_id,
        sum(if(sum_order_count >0 , 1, 0)) c1,
        sum(if(sum_order_count >1 , 1, 0)) c2,
        sum(if(sum_order_count >1 , 1, 0)) / sum(if(sum_order_count >0 , 1, 0)) c3,
        rank() over(partition by user_level order by sum(if(sum_order_count >1 , 1, 0)) / sum(if(sum_order_count >0 , 1, 0)) desc ) rk
    from tmp_count
    group by user_level, sku_id
)tmp
where rk<=10


/*每个用户最近一次购买时间

用户宽表
*/

insert overwrite table ads_user_last_pay
select
    if(dua.user_id is not null, dua.user_id, aulp.user_id),
    if(dua.user_id is not null, '2019-11-19', aulp.pay_date)
from (
    select * from dws_user_action where dt='2019-11-19' and payment_count > 0
) dua
full join ads_user_last_pay aulp
on dua.user_id=aulp.user_id






