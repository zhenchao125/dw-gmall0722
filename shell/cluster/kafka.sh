#!/bin/bash

case $1 in
    start)
        for host in hadoop102 hadoop103 hadoop104 ; do
            echo "========== $host ========="
        ssh $host "source /etc/profile; nohup /opt/module/kafka_2.11-0.11.0.2/bin/kafka-server-start.sh /opt/module/kafka_2.11-0.11.0.2/config/server.properties >/opt/module/kafka_2.11-0.11.0.2/bin/kafka-server-start.sh /opt/module/kafka_2.11-0.11.0.2/config/server.properties/kafka.log 2>&1
   & "
        done

       ;;
    stop)
            for host in hadoop102 hadoop103 hadoop104 ; do
                echo "========== $host ========="
                ssh $host "source /etc/profile; nohup /opt/module/kafka_2.11-0.11.0.2/bin/kafka-server-stop.sh "
            done

           ;;

    *)
        echo "你启动的姿势不对"
        echo "  start   启动zookeeper集群"
        echo "  stop    停止zookeeper集群"

    ;;
esac