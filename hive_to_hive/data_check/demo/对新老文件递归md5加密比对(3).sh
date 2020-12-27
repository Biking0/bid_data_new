#!/bin/bash
#进入新集群需对比目录
cd /root/demo
#将该文件夹下所有文件递归md5加密后写入md5.1中(如子文件夹下无文件,将不加入比对)
find ./ -type f -print | xargs md5sum > /root/warehouse/md5.1
#将加密文件排序
cd /root/warehouse
cat md5.1 | sort > md5.1.1
rm md5.1
mv md5.1.1 md5.1

#免密登录至老集群进行相应操作
ssh root@node02 'cd /root/data;find ./ -type f -print | xargs md5sum > /root/warehouse/md5.2;cd /root/warehouse;cat md5.2 | sort > md5.2.1;rm md5.2;mv md5.2.1 md5.2;sc    p /root/warehouse/md5.2 node03:/root/warehouse/md5.2'

#对比新老md5加密文件
if [ -z "`diff md5.1 md5.2`" ];then
	echo "it is ok"
	echo "$(date +"%Y-%m-%d %H:%M:%S")  数据下载成功并且MD5校验通过" >> mail1.log
else
	echo "they are different"
	echo "$(date +"%Y-%m-%d %H:%M:%S")  数据下载异常或者MD5校验未通过" >> mail1
	#输出不同行在文件哪儿个位置写入different文件
	#7c7
	#< d41d8cd98f00b204e9800998ecf8427e  ./c.txt
	#---
	#> d41d8cd98f00b204e9800998ecf8427e  ./d.txt
	#上面的"7c7"表示两文件在第7行内容有所不同
	diff md5.1 md5.2 > different
	

	#将文件整个比对结果写入compare_all文件
	#"|"表示前后2个文件内容有不同
	diff md5.1 md5.2 -y -W 150 > compare_all
fi

======================================================
035d593d7aa12c320f1b826fdb2a0725  ./2.txt                                       035d593d7aa12c320f1b826fdb2a0725  ./2.txt
161b8cf08582ef33db43d5d5b480266e  ./a/a.txt                                     161b8cf08582ef33db43d5d5b480266e  ./a/a.txt
161b8cf08582ef33db43d5d5b480266e  ./a.txt                                       161b8cf08582ef33db43d5d5b480266e  ./a.txt
a20f30bbddfdfd01c92ba9b57eddf88f  ./3.txt                                       a20f30bbddfdfd01c92ba9b57eddf88f  ./3.txt
c7211fd070de77f13c830ae1dbd0e32a  ./t_access_times.dat                          c7211fd070de77f13c830ae1dbd0e32a  ./t_access_times.dat
d41d8cd98f00b204e9800998ecf8427e  ./a/b/b.txt                                   d41d8cd98f00b204e9800998ecf8427e  ./a/b/b.txt
d41d8cd98f00b204e9800998ecf8427e  ./c.txt                                 |     d41d8cd98f00b204e9800998ecf8427e  ./d.txt

====================创建表，把结果导入到hive表中=======================
drop table if exists check_table;
create table check_table (
id int,
file varchar,
path varchar
)
comment '核对稽查表'
row format delimtied fields terminated by '|';

load data local inpath './compare_all' into table check_table;

=============================================================

nohup  hive -hiveconf   -f  check_table.sql > check_table.log 2>&1