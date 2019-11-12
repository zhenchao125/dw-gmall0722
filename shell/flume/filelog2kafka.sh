#!/bin/bash
flume_home=/opt/module/flume-1.7.0/

case $1 in
    start)
        for host in hadoop102 hadoop103 ; do
            echo "========== $host ========="
            ssh $host "source /etc/profile ; nohup $flume_home/bin/flume-ng agent -n a1 -c $flume_home/conf -f /opt/dw0722/flume/filelog2kafka.conf >/dev/null 2>&1 & "
        done
       ;;

   stop)
        for host in hadoop102 hadoop103 ; do
            echo "========== $host ========="
            ssh $host "source /etc/profile ; ps -ef | awk '/filelog2kafka.conf/ && !/awk/ {print \$2}' | xargs kill -9"
        done
        ;;
    *)
        echo "你启动的姿势不对"
        echo "  start   启动flume 从logfilke到kafka的数据采集"
        echo "  stop    停止flume 从logfilke到kafka的数据采集"

    ;;
esac

# ps -ef | grep filelog2kafka.conf | grep -v grep | awk '{print $2}' | xargs kill -9