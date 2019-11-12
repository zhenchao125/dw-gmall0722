#!/bin/bash
flume_home=/opt/module/flume-1.7.0/

case $1 in
    start)
        echo "========== hadoop104 ========="
        ssh hadoop104 "source /etc/profile ; nohup $flume_home/bin/flume-ng agent -n a1 -c $flume_home/conf -f /opt/dw0722/flume/kafka2hdfs.conf >/dev/null 2>&1 & "
       ;;
    stop)
        echo "========== hadoop104 ========="
        ssh hadoop104 "source /etc/profile ; ps -ef | awk '/kafka2hdfs.conf/ && !/awk/ {print \$2}' | xargs kill -9"
       ;;
    *)
        echo "你启动的姿势不对"
        echo "  start   启动flume 从kafka到hdfs的数据采集"
        echo "  stop    停止flume 从kafka到hdfs的数据采集"

    ;;
esac

# ps -ef | grep filelog2kafka.conf | grep -v grep | awk '{print $2}' | xargs kill -9