#!/bin/bash

if [[ $# == 0  ]]; then
    echo '请输入你要分发的文件或路径'
    exit
fi

# 文件名

fileName=`basename $1`
echo $fileName

# 绝对路径

dir=`cd -P $(dirname $1); pwd`

# 当前的用户名
user=`whoami`


for host in hadoop103 hadoop104 ; do
    echo =========== $host ==========
    rsync -rvl $dir/$fileName $user@$host:$dir
done
