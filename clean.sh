#!/bin/bash

#使环境变量生效
source /etc/profile
source /etc/sysconfig/i18n #中文显示

#目录名
dir=$(date +%m-%d -d -10days)

#不存在这个目录就退出
if [ ! -d "/media/sdb1_2TB/BB-Data/$dir" ];then
exit 0
fi

#进入目录
cd /media/sdb1_2TB/BB-Data//$dir

if [ -d "var" ];then
rm -rf var
fi

for i in *.log
do
rm -rf *.log
done


for i in *.zip
do
rm -f *.zip

done
