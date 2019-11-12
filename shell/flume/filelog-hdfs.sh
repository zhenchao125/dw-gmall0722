#!/bin/bash

/opt/dw0722/flume/kafka2hdfs.sh $1
case $1 in
    start)
        sleep 5
   ;;
esac
/opt/dw0722/flume/filelog2kafka.sh $1