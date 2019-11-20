#!/bin/bash
sqoop=/opt/module/sqoop-1.4.6/bin/sqoop

$sqoop export \
--connect jdbc:mysql://hadoop102:3306/gmall  \
--username root \
--password aaaaaa \
--table ads_gmv_sum_day \
--num-mappers 1 \
--export-dir /warehouse/gmall/ads/ads_gmv_sum_day \
--fields-terminated-by '\t'   \
--update-mode allowinsert \
--update-key 'dt'  \
--input-null-string '\\N' \
--input-null-non-string '\\N'







