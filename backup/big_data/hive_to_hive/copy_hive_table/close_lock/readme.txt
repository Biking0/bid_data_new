20200819
hadoop集群hive库事务表关闭方案验证

1.导出表结构：
show create table ;
导出建表语句到文件，create_table.sql。

2.老表rename，老表名，tb_test，新表名，tb_test_bak：
hdfs文件路径会变更，数据会相应移到新目录。

3.利用导出的表结构文件创建新表：
beeline -f create_table.sql

4.移动数据：
hadoop fs -cp -R /tb_test_bak/* /tb_test/

5.修复表：
msck repair table tb_test;