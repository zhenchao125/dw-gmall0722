#!/bin/bash

sqoop=/opt/module/sqoop-1.4.6/bin/sqoop
table_name=$1
if [[ -n $2 ]]; then
    do_date=$2  # 导入的具体日期是第二个参数
else
    do_date=`date -d '-1 day' +%F`
fi

function import_data(){
    table=$1
    sql=$2  # 传递给函数的第一个参数

    $sqoop import \
    --connect jdbc:mysql://hadoop102:3306/gmall \
    --username root \
    --password aaaaaa \
    --num-mappers 1 \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --query  "${sql} and \$CONDITIONS" \
    --target-dir /origin_data/gmall/db/${table}/${do_date} \
    --fields-terminated-by '\t' \
    --delete-target-dir
}

function import_user_info() {
    import_data "user_info" "
                            select
id, name, birthday, gender, email, user_level,
create_time
from user_info where 1=1
                            "
}

function import_order_info() {
    import_data "order_info" "
select
    id,
    total_amount,
    order_status,
    user_id,
    payment_way,
    out_trade_no,
    create_time,
    operate_time
  from order_info
  where (DATE_FORMAT(create_time,'%Y-%m-%d')='$do_date' or DATE_FORMAT(operate_time,'%Y-%m-%d')='$do_date')

                              "
}

function import_payment_info() {
    import_data "payment_info" "
select
    id,
    out_trade_no,
    order_id,
    user_id,
    alipay_trade_no,
    total_amount,
    subject,
    payment_type,
    payment_time
  from payment_info
  where DATE_FORMAT(payment_time,'%Y-%m-%d')='$do_date'

                                "
}

import_sku_info(){
  import_data "sku_info" "select
id, spu_id, price, sku_name, sku_desc, weight, tm_id,
category3_id, create_time
  from sku_info where 1=1"
}

import_base_category1(){
  import_data "base_category1" "select
id, name from base_category1 where 1=1"
}

import_base_category2(){
  import_data "base_category2" "select
id, name, category1_id from base_category2 where 1=1"
}

import_base_category3(){
  import_data "base_category3" "select id, name, category2_id from base_category3 where 1=1"
}

import_order_detail(){
  import_data   "order_detail"   "select
    od.id,
    order_id,
    user_id,
    sku_id,
    sku_name,
    order_price,
    sku_num,
    o.create_time
  from order_info o, order_detail od
  where o.id=od.order_id
  and DATE_FORMAT(create_time,'%Y-%m-%d')='$do_date'"
}


# 整个脚本的第一个参数是表明
case ${table_name} in
    "base_category1")
        import_base_category1
    ;;
      "base_category2")
         import_base_category2
    ;;
      "base_category3")
         import_base_category3
    ;;
      "order_info")
         import_order_info
    ;;
      "order_detail")
         import_order_detail
    ;;
      "sku_info")
         import_sku_info
    ;;
      "user_info")
         import_user_info
    ;;
      "payment_info")
         import_payment_info
         ;;
      "all")
        import_base_category1
       import_base_category2
       import_base_category3
       import_order_info
       import_order_detail
       import_sku_info
       import_user_info
       import_payment_info
      ;;

esac

