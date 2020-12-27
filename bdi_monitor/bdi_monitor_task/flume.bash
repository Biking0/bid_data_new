#!/bin/bash



EXEC_MYSQL_NG="mysql -h10.97.192.180 -ungtassuite -pAj7y32h! ngtassuite"
function send_sms()
{
        ${EXEC_MYSQL_NG} -e "INSERT INTO tb_sys_sms_send_cur(serv_number, send_date,text,opt_user,opt_date ) SELECT serv_number, now(), '$1', opt_user, now() FROM tb_sys_sms_phone WHERE opt_user = 'ocetl' and sms_id = '$2';"
}



for i in flume-agent1 flume-agent2 flume-agent3
do
        ssh 10.218.146.103 "tail -3000 /var/log/flume/$i.log > /home/ocdp/wangcc/$i.txt"

        num=`ssh 10.218.146.103 "grep waiting /home/ocdp/wangcc/$i.txt | wc -l"`

        current_time=`date +"%Y-%m-%d %H:%M"`

        if [ $num -eq 0 ] ;then
                echo "$i 日志更新正常,当前时间为：$current_time"
                send_sms "$i 日志更新正常,当前时间为：$current_time" "w01"           
        else
                echo "$i 最近10分钟日志文件中有$num个waiting报警，请查看,当前时间为：$current_time"
                send_sms "$i 最近10分钟日志文件中有$num个waiting报警，请查看,当前时间为：$current_time" "w01"

        fi

done






echo "$i 日志更新正常,当前时间为：$current_time"
                #send_sms "$i 日志更新正常,当前时间为：$current_time" "w01"           
        else
                echo "$i 最近10分钟日志文件中有$num个waiting报警，请查看,当前时间为：$current_time"
                send_sms "$i 最近一千条日志文件中有$num个waiting报警，请查看,当前时间为：$current_time" "w01"
