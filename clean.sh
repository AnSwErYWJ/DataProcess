#!/bin/bash

source /etc/profile
source /etc/sysconfig/i18n

dir=$(date +%m-%d -d -10days)
new=$(date +%Y-%m-%d -d -10days)

if [ ! -d "/media/sdb1_2TB/BB-Data/$dir" ];then
exit 0
fi

cd /media/sdb1_2TB/BB-Data//$dir

if [ -d "var" ];then
rm -rf var
fi

for i in $new*
do
rm -rf $i
done

for i in *.zip
do
rm -f *.zip

done
