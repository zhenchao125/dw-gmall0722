#!/bin/bash

for host in hadoop102 hadoop103 ; do
    echo ========== $host =========
    ssh $host "source /etc/profile ; nohup java -jar /opt/dw0722/data-producer/data-producer-1.0-SNAPSHOT-jar-with-dependencies.jar 1>/dev/null 2>1 &"
done
