20201215
临时新老库数据稽核

# 苏研生产环境
mysql -A -h192.168.214.185 -P20031 -uhive hive  -p39\)6aUCs


ocdp集群hive元数据库
mysql -h ritds-hive.mysql.svc.cs1-hua.hpc -P 20001 -u hive -p775K6zZuTGk1uq6e

select count(*) from PARTITIONS where TBL_ID = ( select TBL_ID from TBLS where OWNER='hive' and tbl_name='test'
 order by CREATE_TIME desc limit 1);




