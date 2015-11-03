#!/bin/bash

#crontab 环境变量生效
source /etc/profile
source /etc/sysconfig/i18n #中文显示

cd /media/sdb1_2TB/BB-Data/data_process #进入目录

yesterday=$(date +%Y-%m-%d -d -2days) 
dir_ftp=$(date +%Y%m%d -d -2days) #上传目录名
dir_name=$(date +%m-%d -d -2days) #本地目录名

list="../$dir_name/url-$yesterday.txt" #下载链接文件

#创建对应目录
if [ ! -d "../$dir_name" ];then
mkdir ../$dir_name
fi

#生成相应下载链接
awk -F_ '{
            $NF="'$yesterday'.log.tgz"
            OFS="_" 
            print
        }' raw.txt > $list


#批量下载
start_time=$(date +%s)
wget -c --accept=tgz --http-user=optimizerdl_auth --http-passwd=S0mO4Ei8LjWzkcoeqZtt -i $list -P ../$dir_name
end_time=$(date +%s)
total_time=`expr $end_time - $start_time`
total_min=`expr $total_time / 60`
total_sec=`expr $total_time % 60`
echo "下载耗时:$total_min 分 $total_sec 秒" > ../$dir_name/time.txt

#sleep 3s

#批量解压
start_time=$(date +%s)
for file in ../$dir_name/*.tgz
do
    tar -zxvf $file -C ../$dir_name
done
end_time=$(date +%s)
total_time=`expr $end_time - $start_time`
total_min=`expr $total_time / 60`
total_sec=`expr $total_time % 60`
echo "解压耗时:$total_min 分 $total_sec 秒" >> ../$dir_name/time.txt

#sleep 3s

#批量解析
cd ../$dir_name
start_time=$(date +%s)
for log in var/www/neo/logs/*.log
do
 	/usr/local/bin/python3 ../data_process/jsontransform.py $log config
done
end_time=$(date +%s)
total_time=`expr $end_time - $start_time`
total_min=`expr $total_time / 60`
total_sec=`expr $total_time % 60`
echo "解析耗时:$total_min 分 $total_sec 秒" >> time.txt

sleep 3s

ADDR=111.203.211.2
PORT=1503
USER="ftp_up01"
PWD="dk3_gk3up"

# 批量压缩
start_time=$(date +%s)
for dir in *.log
do
zip -r $dir.zip $dir
done
end_time=$(date +%s)
total_time=`expr $end_time - $start_time`
total_min=`expr $total_time / 60`
total_sec=`expr $total_time % 60`
echo "压缩耗时:$total_min 分 $total_sec 秒" >> time.txt

#上传
#start_time=$(date +%s)
#	ftp  -n <<EOF 
#	open $ADDR $PORT
#	user $USER $PWD
#	binary
#	mkdir /$dir_ftp
#	cd /$dir_ftp/
#	lcd /media/sdb1_2TB/BB-Data/$dir_name
#	hash
#	prompt
#	put *.zip
#	close
#	bye
#EOF
#end_time=$(date +%s)
#total_time=`expr $end_time - $start_time`
#total_min=`expr $total_time / 60`
#total_sec=`expr $total_time % 60`
#echo "上传耗时:$total_min 分 $total_sec 秒" >> time.txt


