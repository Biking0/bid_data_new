#!/bin/bash
# ***************************************************************************
# 文件名称：check_flume_log.sh
# 功能描述：shell代码头
# 输 入 表：
# 输 出 表：
# 创 建 者：
# 创建日期：
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：sh code_head.sh
# ***************************************************************************

EXEC_MYSQL_NG="mysql -h10.97.192.180 -ungtassuite -pAj7y32h! ngtassuite"
function send_sms()
{
        ${EXEC_MYSQL_NG} -e "INSERT INTO tb_sys_sms_send_cur(serv_number, send_date,text,opt_user,opt_date ) SELECT serv_number, now(), '$1', opt_user, now() FROM tb_sys_sms_phone WHERE opt_user = 'ocetl' and sms_id = '$2';" 2>/dev/null
}



for i in flume-agent1 flume-agent2
do
  ssh 10.218.146.103 "tail -3000 /var/log/flume/$i.log > /home/ocdp/wangcc/$i.txt"

  num=`ssh 10.218.146.103 "grep waiting /home/ocdp/wangcc/$i.txt | wc -l"`

  error_num=`ssh 10.218.146.103 "grep ERROR /home/ocdp/wangcc/$i.txt | wc -l"`

  current_time=`date +"%Y-%m-%d %H:%M"`

  if [ $num -eq 0 -a $error_num -eq 0 ] ;then
          echo "$i 日志更新正常,当前时间为：$current_time"
          #send_sms "$i 日志更新正常,当前时间为：$current_time" "w01"
  else
     echo "$i 最近30分钟日志文件中有 $num 个waiting报警和 $error_num 个ERROR报警,请查看, 当前时间为： $current_time "
     send_sms "$i最近30分钟日志文件中有$num个waiting报警和$error_num个ERROR报警,请查看, 当前时间为：$current_time" "ALL"

        fi

done
