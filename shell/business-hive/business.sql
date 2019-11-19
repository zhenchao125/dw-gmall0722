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



