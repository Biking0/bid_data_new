#!/bin/bash
EXEC_MYSQL_NG="mysql -h10.97.192.180 -ungtassuite -pAj7y32h! ngtassuite"
function send_sms()
{
        ${EXEC_MYSQL_NG} -e "INSERT INTO tb_sys_sms_send_cur(serv_number, send_date,text,opt_user,opt_date ) SELECT serv_number, now(), '$1', opt_user, now() FROM tb_sys_sms_phone WHERE opt_user = 'ocetl' and sms_id = '$2';"
}

ipcon=/hdfs/data02/oc_tools/disk_monitor_90/disk_ipconf_90.cnf
for monitor_ip in `cat ${ipcon} | grep diskmonitor | awk -F ','  '{print $1}'`
        do 
ssh ${monitor_ip} df -hP | grep -v 'Size' | grep -v 'html' | awk '{print $5,$6}' | awk -F '%' '{print $1,$2}'  >/hdfs/data02/oc_tools/disk_monitor_90/log/${monitor_ip}_disk_use.cnf
line=`cat /hdfs/data02/oc_tools/disk_monitor_90/log/${monitor_ip}_disk_use.cnf | wc -l`
for ((i=1;i<=${line};i++));
        do
            used=`awk 'NR=="'$i'" {print $1}' /hdfs/data02/oc_tools/disk_monitor_90/log/${monitor_ip}_disk_use.cnf`
            mount=`awk 'NR=="'$i'" {print $2}' /hdfs/data02/oc_tools/disk_monitor_90/log/${monitor_ip}_disk_use.cnf`
                if [ ${used} -gt  80 ];then
                        send_sms "${monitor_ip}服务器${mount}磁盘已使用${used}%，当前时间：`date +"%m-%d %H:%M"`。"  "ALL"
                        echo "${monitor_ip}服务器${mount}磁盘已使用${used}%" ，当前时间：`date +"%m-%d %H:%M"`。>>/hdfs/data02/oc_tools/disk_monitor_90/log/send_disk_use.log
                else echo "${monitor_date} ${monitor_ip}服务器${mount}磁盘已使用${used}%,属于正常情况，当前时间：`date +"%m-%d %H:%M"`。" >>/hdfs/data02/oc_tools/disk_monitor_90/log/all_disk_use.log
                fi
        done
  done
