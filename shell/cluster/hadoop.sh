#!/bin/bash

case $1 in
    start)
        echo ========== hadoop102 上启动hdfs ==========
        ssh hadoop102 "source /etc/profile ; start-dfs.sh"

        echo ========== hadoop103 上启动yarn ==========
        ssh hadoop103 "source /etc/profile ; start-yarn.sh"
       ;;

    stop)

        echo ========== hadoop103 上停止yarn ==========
        ssh hadoop103 "source /etc/profile ; stop-yarn.sh"

        echo ========== hadoop102 上停止hdfs ==========
        ssh hadoop102 "source /etc/profile ; stop-dfs.sh"

     ;;
    *)
        echo "你启动的姿势不对"
        echo "  start 启动hadoop集群"
        echo "  stop  停止hadoop集群"
    ;;

esac