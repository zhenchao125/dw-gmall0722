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
insert overwrite table ads_user_last_pay
select
    if(dua.user_id is not null, dua.user_id, aulp.user_id),
    if(dua.user_id is not null, '$do_date', aulp.pay_date)
from (
    select * from dws_user_action where dt='$do_date' and payment_count > 0
) dua
full join ads_user_last_pay aulp
on dua.user_id=aulp.user_id;
"

$hive -e "$sql"


