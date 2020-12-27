[ocdp@hnbd090 sendsms]$ cat sendsms.sh
#!/bin/sh
. ~/.bash_profile

SQL_TEXT="/home/ocetl/oc_tools/sendsms/.sms.sql-`date +%M`"

EXEC_MYSQL="mysql -h10.97.192.180 -ungtassuite -pAj7y32h! ngtassuite "
EXEC_ORACLE="sqlplus bomcinterface/tgTG#0319"

echo "select concat( 'insert into bomcitsm.itf_sms values( NULL, \'', serv_number, '\', \'',text, '\', \'1\', sysdate, \'jingfen_LL\',sysdate,\'',dbid_,'\', \'NORMAL\');') sql_clause from tb_sys_sms_send_cur;" | $EXEC_MYSQL | grep "insert" >${SQL_TEXT}
echo "SQL_TEXT:${SQL_TEXT}"
if [ -s ${SQL_TEXT} ]
then
sqlplus bomcinterface/tgTG#0319@bomc1 <<EOF
@ ${SQL_TEXT}
commit;
exit;
EOF
#LINE_MAX=`awk -F "'" '{if(M<$10)M=$10;}END{print M }' ${SQL_TEXT}`
LINE_MAX=`awk -F "'" '{print $10 }' ${SQL_TEXT}`
    #echo "LINE_MAX:${LINE_MAX}"
    for x in $LINE_MAX
    do
    echo "`date +%m-%d\ %H:%M` dbid_ ='${x}'"
$EXEC_MYSQL -e "insert into tb_sys_sms_send_his select * from tb_sys_sms_send_cur where dbid_ ='${x}';delete from tb_sys_sms_send_cur where dbid_ ='${x}';commit;"
    done
fi


