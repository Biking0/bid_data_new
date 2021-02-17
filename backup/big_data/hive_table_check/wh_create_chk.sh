#!/bin/sh
source /opt/fi_client/bigdata_env
#    #dw_locl_wh_lacci_4g_time_yyyymmddhh
#    tab_name=$1
#    end_time=$3
#    month_id=${2:0:6}
#    day_id=${2:0:8}
#    end_hour=$day_id$end_time
#    
#do_hive ()
#{
#    hadoop fs -du -s -h /user/hive/warehouse/asiainfoh.db/$tab_name/month_id=$month_id/day_id=$day_id/hour_id=$end_hour
#    if [ $? == '0' ];then
#       break
#    fi
#}
#
#while true
#do
#   echo $i
#   do_hive $i
#   sleep 2
#   echo 'no'
#done

# 华为服务器认证
source /opt/fi_client/bigdata_env
#kinit -kt /home/bdi/user.keytab asiainfouser1

# 输入参数处理
table_name=$1
data_time=$2
#running_num=$3
#echo ${running_num} > ${table_name}_${data_time}.txt

# 创建校验文件
hadoop fs -touchz /asiainfo/dependent/${table_name}.txt
# hadoop fs -put /asiainfo/dependent/${table_name}_${data_time}.txt

#hadoop fs -put ./${table_name}_${data_time}.txt /asiainfo/dependent/
#hadoop fs -touchz /asiainfo/dependent/${table_name}.txt

echo "ok"
