#!/bin/bash

if [[ -n $1 ]]; then
    cmd=$1
else
    cmd=jps
fi

for host in hadoop102 hadoop103 hadoop104 ; do
    echo "========== $host ========="
    ssh $host "source /etc/profile; $cmd"
done

