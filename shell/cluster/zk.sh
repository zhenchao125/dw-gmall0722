#!/bin/bash

case $1 in
    start | stop | status)
        for host in hadoop102 hadoop103 hadoop104 ; do
            echo "========== $host ========="
        ssh $host "source /etc/profile; /opt/module/zookeeper-3.4.10/bin/zkServer.sh $1"
        done

       ;;

    *)
        echo "你启动的姿势不对"
        echo "  start   启动zookeeper集群"
        echo "  stop    停止zookeeper集群"
        echo "  status  查看zookeeper集群"
    ;;
esac