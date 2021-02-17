#!/bin/sh 
#======================把hive库的所有表的结构导出来========================================
#---------先把所有的表名写入到一个文件中
#list_tables.sql  >>  show tables  
#
#---------显示所有的表结构写入到一个文件中---------
#show_create.sql  >>  show create table  
#
#-------------------导出新系统所有hive的表结构-------------
DATABASES='aa'
for databases in ${DATABASES}
do
nohup hive -hiveconf database=${databases} -S -f  list_tables.sql > ${databases}_new_tables_name.txt 2>&1
    cat ${databases}_new_tables_name.txt | while read eachline
    do
        hive -hiveconf database=${databases} -hiveconf table=${eachline} -S -f show_create.sql >> ${databases}_new_tables_structure.txt
        echo >> ${databases}_new_tables_structure.txt
    done
done	


#----------------导出新系统vertica数据库的所有表结构------------------
#从vertica数据库中显示所有的表导出到一个文件，shell命令：
echo `vsql -d dbname -U dbadmin -Atq -w xx -c "show tables" >> /database/datastage/export/dim_all/new_test`;


for line in `cat new_test` 
do   
  echo $line     
  echo `vsql -d dbname -U dbadmin -Atq -w xx -c "show create table $line" >> /database/datastage/export/dim_all/new_test1`;
done


#-------------------导出老系统所有hive的表结构-------------
DATABASES=''
for databases in ${DATABASES}
do
nohup hive -hiveconf database=${databases} -S -e  "show table" > ${databases}_old_tables_name.txt 2>&1
    cat ${databases}_old_tables_name.txt | while read eachline
    do
        hive -hiveconf database=${databases} -hiveconf table=${eachline} -S -e "show create " >> ${databases}_old_tables_structure.txt
        echo >> ${databases}_old_tables_structure.txt
    done
done

#----------------导出老系统vertica数据库的所有表结构------------------
#从vertica数据库中显示所有的表导出到一个文件，shell命令：
echo `vsql -d dbname -U dbadmin -Atq -w xx -c "show tables" >> /database/datastage/export/dim_all/old_test`;

for line in `cat old_test` 
do   
  echo $line     
  echo `vsql -d dbname -U dbadmin -Atq -w xx -c "show create table $line" >> /database/datastage/export/dim_all/old_test1`;
done

#--------------------对新老系统vertica数据库两个文件进行比较-------------
diff -r new_test1 old_test1
if [ $? != 0 ];then
echo "MD5校验未通过" >> /root/aa.log
echo "文件有差异"
else 
echo "MD5校验通过" >> /root/aa.log
echo "文件比对通过"
fi

#-------------------对新老系统hive数据库两个文件进行比较---------------
diff -r ${databases}_old_tables_structure.txt ${databases}_new_tables_structure.txt
if [ $? != 0 ];then
echo "MD5校验未通过" > /root/bb.log
echo "文件有差异"
else 
echo "MD5校验通过" > /root/bb.log
echo "文件比对通过"
fi

#==================把新老系统核对的结果导入表中===============
drop table if exists check_composition_table;
create table check_composition_table(
id int,
table_name verchar(32),
different varchar(32)
	)
comment "表结构核对稽查表"
row format delimtied fields terminated by '';

hive -e 'load data local inpath '/root/aa.log' into table check_composition_table';

hive -e 'load data local inpath '/root/bb.log' into table check_composition_table';










