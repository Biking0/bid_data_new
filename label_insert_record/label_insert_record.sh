#!/bin/bash
# ***************************************************************************
# 文件名称：label_insert_record.sh
# 功能描述：标签label库插入记录
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20200428
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：sh label_insert_record.sh table_name date
# ***************************************************************************

# 传入参数
table_name=$1
date=$2

SQL_TEXT="/home/ocetl/oc_tools/sendsms/label.sql-`date +%Y%m%d%H%M`"

sql="insert into dim_target_table_status(source_table_id,table_schema,source_table_name,data_date,data_status)values('0','label','${table_name}','${date}','1');"

echo $sql

echo $sql > ${SQL_TEXT}


# 登录oracle执行sql语句
if [ -s ${SQL_TEXT} ] 
then
sqlplus bd_label/'4rfv$RFV'@BD_LABEL <<EOF
@ ${SQL_TEXT}
commit;
exit;
EOF

fi

