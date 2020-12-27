#!/bin/bash
# ***************************************************************************
# 文件名称：check_search_log.sh
# 功能描述：监控search日志，出现waiting，error告警
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：2020325
# 修改日志：调大ERROR监控阈值
# 修改日期：2020419
# ***************************************************************************
# 程序调用格式：sh check_search_log.sh
# ***************************************************************************

# 短信数据库登录信息
EXEC_MYSQL_NG="mysql -h10.97.192.180 -ungtassuite -pAj7y32h! ngtassuite"

# 发送短信
function send_sms()
{
    ${EXEC_MYSQL_NG} -e "INSERT INTO tb_sys_sms_send_cur(serv_number, send_date,text,opt_user,opt_date ) SELECT serv_number, now(), '$1', opt_user, now() FROM tb_sys_sms_phone WHERE opt_user = 'ocetl' and sms_id = '$2';" 2>/dev/null
}

# 监控日志行数
log_line=500

# error和waitting监控警戒值
monitor_num=5

# 过滤发送人
send_who='ALL'
#send_who='HYN'

# 循环监控65、66机器
for i in 65 66
do 
	#echo 1
	# 监测65、66 search log
	ssh 10.218.146.$i "tail -$log_line /home/ocdp/ocsearch/server/logs/ocsearch_all.log > /home/ocdp/hyn/ocsearch_log/search_log_$i.txt" 
	
	# 获取日志最新时间
	# ssh 10.218.146.$i "tail -1 /home/ocdp/ocsearch/server/logs/ocsearch_all.log | awk '{print $1$2} ' > /home/ocdp/hyn/ocsearch_log/search_log_time_$i.txt" 
	
	#log_time=`ssh 10.218.146.$i "cat /home/ocdp/hyn/ocsearch_log/search_log_time_$i.txt | awk '{print $1,$2}'"`
	#echo $log_time
	#test="ssh 10.218.146.$i "grep waiting /home/ocdp/hyn/search_log/search_log_$i.txt | wc -l"
	
	#echo 2
	waiting_num=`ssh 10.218.146.$i "grep waiting /home/ocdp/hyn/ocsearch_log/search_log_$i.txt | wc -l"`
	#echo 3
	error_num=`ssh 10.218.146.$i "grep ERROR /home/ocdp/hyn/ocsearch_log/search_log_$i.txt | wc -l"`
	#echo 4
	current_time=`date +"%Y-%m-%d %H:%M"`
	
	echo $i waiting数量：$waiting_num,error数量：$error_num
	
	# 如果出现waiting、error信息发送告警信息
	if [ $waiting_num -le $monitor_num -a $error_num -le $monitor_num ] ;then
		echo "$i 日志更新正常,当前时间为：$current_time"
		#send_sms "$i 日志更新正常,当前时间为：$current_time" "w01"
	else
		#sms_info="$i search日志最新$log_line行有$waiting_num个waiting和$error_num个ERROR报警,请查看，当前时间为：$current_time"
		sms_info="$i机器search日志最新$log_line行有$waiting_num个waiting和$error_num个ERROR报警,请查看，当前时间为：$current_time"
		echo $sms_info
		send_sms "$i机器search日志最新$log_line行有$waiting_num个waiting和$error_num个ERROR报警,请查看，当前时间为：$current_time" $send_who
		#send_sms "$i机器search日志最新$log_line行有$waiting_num个waiting和$error_num个ERROR报警,请查看，当前时间为：$current_time" "ALL"
		#send_sms "测试ocsearch" "HYN"
	fi
	
done



