#!/bin/bash
#sh cp_82.sh dw_hangti_lac_user_yyyymmdd 20181006  /home/bdi/Asiainfo/tas/ht

source /opt/fi_client/bigdata_env
kinit -kt /home/bdi/user.keytab asiainfouser1

table_name=$1
day_id=$2
month_id=${day_id:0:6}

if [ $? -eq 0 ] ;then
echo "10.218.59.8 is active"
DATA_FROM='10.218.59.8'
else
echo "10.218.59.7 is active"
DATA_FROM='10.218.59.7'
fi
hadoop fs -ls -d hdfs://10.93.171.100:8020/user/hive/warehouse/ &>/dev/null
if [ $? -eq 0 ] ;then
echo "10.93.171.100 is active"
DATA_TO='10.93.171.100'
else
echo "10.93.171.97 is active"
DATA_TO='10.93.171.97'
fi
hadoop distcp -i -pb /user/hive/warehouse/asiainfoh.db/${table_name}/month_id=${month_id}/day_id=${day_id}/* hdfs://$DATA_TO:8020/user/hive/warehouse/${table_name}/month_id=${month_id}/day_id=${day_id}/
