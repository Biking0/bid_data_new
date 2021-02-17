#!/bin/bash
# ***************************************************************************
# 文件名称：dw_label_quanyi_yyyymm_ftp110.sh
# 功能描述：传数据到接口机110
# 输 入 表：10.93.171.83，ocetl
# 输 入     dw_label_quanyi_yyyymm_ftp110
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20200228
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：sh test.sh test 20191231
# 程序调用格式：sh dw_label_quanyi_yyyymm_ftp110.sh dw_label_quanyi_yyyymm 202002
# ***************************************************************************

# 后台运行
#set -x

# 输入参数处理
table_name=$1
month_id=$(echo $2|cut -c1-6 )
#day_id=$2


# 数据本地存放路径，从hdfs下载数据存放到本地
local_data_path="/hdfs/data9/port_files/"

data_path=${local_data_path}dw_label_quanyi_${month_id}

mkdir $data_path

#touch ${local_data_path}dw_label_quanyi1_${month_id}/dw_label_quanyi_${month_id}.txt

#echo ${table_name}_$month_id
#echo ${local_data_path}${table_name}_$month_id.txt

# 创建数据存放文件
echo ${local_data_path}dw_label_quanyi_${month_id}/dw_label_quanyi_${month_id}.txt

# hive表导出到文件
hive -e "set hive.exec.compress.output=false;set hive.cli.print.header=false;
select concat(
trim(statis_month),'|',
trim(user_id),'|',
city_id,'|',
trim(phone_no),'|',
trim(label_id)) from dw_label_quanyi_yyyymm where month_id=${month_id} ;" >${local_data_path}dw_label_quanyi_${month_id}/dw_label_quanyi_${month_id}.txt;

# sftp协议传文件到110
sftp ftpintf@10.93.171.110 <<EOF

mkdir /export/home/out_file/quanyi
cd /export/home/out_file/quanyi
lcd ${local_data_path}dw_label_quanyi_${month_id}

binary
mput *

bye
EOF

#binary
#mput *




