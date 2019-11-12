#!/bin/bash

for host in hadoop102 hadoop103 hadoop104 ; do
    echo "========== $host ========="
    ssh $host "source /etc/profile ; sudo date -s $1"
done

