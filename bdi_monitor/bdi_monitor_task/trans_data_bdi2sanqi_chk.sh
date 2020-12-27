#!/bin/bash
# ***************************************************************************
# 文件名称：trans_data_bdi2sanqi_chk.sh
# 功能描述：日表，产生校验文件到大数据平台hdfs数据同步到三期hdfs集群，包括月表、日表、小时表
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20191216
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：sh trans_data_bdi2sanqi_chk.sh table_name data_time running_num
# ***************************************************************************

# 华为服务器认证
source /opt/fi_client/bigdata_env
kinit -kt /home/bdi/user.keytab asiainfouser1

# 输入参数处理
table_name=$1
data_time=$2
running_num=$3
echo ${running_num} > ${table_name}_${data_time}.txt

# 创建校验文件
# hadoop fs -touchz /asiainfo/dependent/${table_name}_${data_time}.txt
# hadoop fs -put /asiainfo/dependent/${table_name}_${data_time}.txt
hadoop fs -put ./${table_name}_${data_time}.txt /asiainfo/dependent/