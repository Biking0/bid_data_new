length_input_time=${#input_time}
if [$length_input_time -eq 6];then
	echo $table_name 'is month_table'
	month_id=$input_time
else if [$length_input_time -eq 8]
	echo $table_name 'is day table'
	month_id=${input_time:0:6}
	day_id=$input_time
else if [$length_input_time -eq 10]
	echo $table_name 'is hour table'
	month_id=${input_time:0:6}
	day_id=${input_time:0:8}
	hour_id=$input_time
else
	echo 'input data error'
	exit
fi

echo $table_name
echo $input_time
echo $month_id
echo $day_id
echo $hour_id


# 确定三期集群活跃节点
hadoop fs -ls -d hdfs://10.93.171.100:8020/user/hive/warehouse/ &>/dev/null
if [ $? -eq 0 ] ;then
	echo "10.93.171.100 is active"
	DATA_TO='10.93.171.100'
else
	echo "10.93.171.97 is active"
	DATA_TO='10.93.171.97'
fi


# 目标集群hdfs目录检查


echo 'bdi_to_sanqi.sh test'