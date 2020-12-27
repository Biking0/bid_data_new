
hadoop fs -ls -d hdfs://10.93.171.97:8020/user/hive/warehouse/ &>/dev/null
if [ $? -eq 0 ] ;
	then
	echo "10.93.171.97 is active"
	DATA_TO='10.93.171.100'
else
	echo "10.93.171.100 is active"
	DATA_TO='10.93.171.97'
fi