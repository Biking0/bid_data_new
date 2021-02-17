
beeline -u 'jdbc:hive2://hua-dlzx2-a0202:10000/default' -n ocdp -p 1q2w1q@W

beeline -u "jdbc:hive2://hua-dlzx2-a0202:10000/default" -n ocdp -p 1q2w1q@W -e "show databases"

# 本地集群
beeline -u "jdbc:hive2://hua-dlzx2-a0202:10000/default" -n ocdp -p 1q2w1q@W
beeline -u "jdbc:hive2://hua-dlzx2-a0202:10000/default" -n ocdp -p 1q2w1q@W -e "show databases"
beeline -u "jdbc:hive2://172.19.168.101:10000/default" -n ocdp -p 1q2w1q@W -e "show databases"

# 远程集群

# 原版
beeline -u "jdbc:hive2://192.168.190.88:10000/default" -n demo -p %Usbr7mx

beeline -u "jdbc:hive2://192.168.190.88:10000/default" -n demo -p %Usbr7mx -e "show databases"

beeline -u "jdbc:hive2://192.168.190.88:10000/default" -n hive -p %Usbr7mx -e "show databases"

beeline -u "jdbc:hive2://192.168.190.88:10000/default" -n ocdp -p 1q2w1q@W -e "show databases"
beeline -u "jdbc:hive2://192.168.190.88:10000/csap"

beeline -u "jdbc:hive2://192.168.190.89:8020/csap" -n ocdp -p 1q2w1q@W -e "show databases"

beeline -u "jdbc:hive2://192.168.190.88:10000/default"

beeline -u "jdbc:hive2://192.168.190.88:10000/csap" -n hive -p %Usbr7mx

beeline -u "jdbc:hive2://192.168.190.89:10000/csap" -n hive -p %Usbr7mx

# 远程主机，进hive
ssh hive@192.168.190.91
%Usbr7mx

ssh hive@192.168.190.91\r\pyes\r\p%Usbr7mx\r\p

# 远程集群ranger
192.168.190.89:6080

# 远程机器上拷贝集群文件
hadoop distcp hdfs://192.168.190.89:50070/apps/hive/warehouse/csap.db/tb_si_cu_voma_limit_whitelist_day/statis_date=20170617 hdfs://172.19.168.4:8020/warehouse/tablespace/managed/hive/tb_si_cu_voma_limit_whitelist_day
hadoop distcp hdfs://192.168.190.89:50070/apps/hive/warehouse/csap.db/tb_si_cu_voma_limit_whitelist_day/statis_date=20170617 hdfs://172.19.168.4/warehouse/tablespace/managed/hive/tb_si_cu_voma_limit_whitelist_day

# 本地测试集群
hadoop distcp hdfs://192.168.190.89:50070/apps/hive/warehouse/csap.db/tb_si_cu_voma_limit_whitelist_day/statis_date=20170617 hdfs://172.22.148.22:8020/warehouse/tablespace/managed/hive/tb_si_cu_voma_limit_whitelist_day

# 测试活跃节点

# 本地
hadoop fs -ls -d hdfs://172.19.168.4:8020/warehouse/tablespace/managed/hive/tb_si_cu_voma_limit_whitelist_day

# 远程
hadoop fs -ls -d hdfs://192.168.190.89:50070/apps/hive/warehouse/csap.db/tb_si_cu_voma_limit_whitelist_day/statis_date=20170617
hadoop fs -ls -d hdfs://192.168.190.89:8020/apps/hive/warehouse/csap.db/tb_si_cu_voma_limit_whitelist_day/statis_date=20170617

# 展示表结构
beeline -u 'jdbc:hive2://192.168.190.88:10000/csap' -e 'show create table tb_si_cu_voma_limit_whitelist_day' > test.txt

show create table tb_si_cu_voma_limit_whitelist_day

# 测试建表

# 远程往本地拷贝数据
hadoop distcp -i hdfs://192.168.190.89:8020/apps/hive/warehouse/csap.db/tb_si_cu_voma_limit_whitelist_day/statis_date=20170617 hdfs://172.19.168.4:8020/warehouse/tablespace/managed/hive/tb_si_cu_voma_limit_whitelist_day


tb_si_cu_voma_limit_whitelist_day

hadoop fs -ls hdfs://192.168.190.89:8020/apps/hive/warehouse/csap.db/tb_si_cu_voma_limit_whitelist_day | more

hadoop fs -ls hdfs://192.168.190.89:8020/apps/hive/warehouse/csap.db/tb_si_cu_voma_limit_whitelist_day | more

sh trans_data_suyan2ocdp.sh tb_si_cu_voma_limit_whitelist_day 20170629 tb_si_cu_voma_limit_whitelist_day

# 登陆vertica


# 测试hive库稽核，hive库关联
create database data_check;
use database_check;
create table test_hyn as select * from default.test_zs_20200529;