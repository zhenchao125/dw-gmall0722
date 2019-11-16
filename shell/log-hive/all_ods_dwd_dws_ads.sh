#!/bin/bash

log_hive=/opt/dw0722/log-hive


# 数据导入到 ods 层
${log_hive}/ods_log.sh $1
# 启动日志导入到 dwd 层
${log_hive}/dwd_start_log.sh $1
# 事件日志
${log_hive}/dwd_base_event_log.sh $1
${log_hive}/dwd_event_log.sh $1

# 活跃主题
${log_hive}/dws_uv_detail.sh $1
${log_hive}/ads_uv_log.sh $1





