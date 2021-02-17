#!/bin/bash
# ***************************************************************************
# 文件名称：trans_data_sanqi2ftp102_dt_day.sh
# 功能描述：三期传数据到ftp102
# 输 入 表：10.93.171.82，ocetl
# 输 入 表：dm_user_incite_label_yyyymmdd
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20191216
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：sh trans_data_sanqi2ftp102_dt_day.sh dm_user_incite_label_dt_yyyymmdd 20200110
# 程序调用格式：sh test.sh test 20191231
# ***************************************************************************

# 后台运行
#set -x

# 输入参数处理
table_name=$1
month_id=$(echo $2|cut -c1-6 )
day_id=$2

# 数据本地存放路径，从hdfs下载数据存放到本地
local_data_path="/hdfs/data9/port_files/"
echo $table_name $month_id $day_id
echo ${local_data_path}${table_name}_$day_id.txt

#*** 获取hadoop中的数据与校验文件
#hadoop fs -get /file_out_bi/dm_label_user_total_yyyymmdd/dm_label_user_total_$1/ /hdfs/data9/port_files
#hive -S -e "select count(*) from dm_label_user_total_yyyymmdd where month_id=$month and day_id=$1 and label_id is not null order by 1 ; "  > /hdfs/data9/port_files/dm_label_user_total_$1/check.verf
#hive -S -e "select concat(trim(catalog_name),'|',
#trim(catalog_id),'|',
#trim(cont_name1),'|',
#trim(cont_id1),'|',
#trim(cont_type_name),'|',
#trim(cont_type_id),'|',
#trim(cont_name2),'|',
#trim(cont_id2),'|',
#trim(label_id),'|',
#trim(label_caliber),'|',
#trim(effect_flag)) from dim_label_id order by 1 ; "  > /hdfs/data9/port_files/dim_label_id/dim_label_id.txt


hive -e "select concat(nvl(statis_time,''),'|'
                      ,nvl(phone_no,''),'|'
                      ,nvl(user_id,''),'|'
                      ,nvl(city_id,''),'|'
                      ,nvl(id,'')) 
         from $table_name where day_id=$day_id" >${local_data_path}dm_user_incite_label_dt_$day_id.txt;

#cd /hdfs/data9/port_files/dm_label_user_total_$1
#for dirlist in $(ls /hdfs/data9/port_files/dm_label_user_total_$1)
#        do
#                mv $dirlist $1_$dirlist
#        done

#*** 传送数据文件以及校验文件
ftp -niv 10.97.192.102 <<EOF
user ftpintf Aj7y32h!


cd /data/ftpintf/out_file/incite
lcd /hdfs/data9/port_files/
binary
mput ./dm_user_incite_label_dt_$day_id.txt

bye
EOF
#*** 删除本地的冗余文件 
rm /hdfs/data9/port_files/dm_user_incite_label_dt_$day_id.txt

