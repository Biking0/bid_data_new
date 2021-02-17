# 测试distcp参数





测试环境
zhumeng

hadoop distcp -i hdfs://192.168.190.89:8020/apps/hive/warehouse/csap.db/tb_si_cu_voma_limit_whitelist_day/statis_date=20170617 hdfs://172.19.168.4:8020/warehouse/tablespace/managed/hive/tb_si_cu_voma_limit_whitelist_day

hadoop distcp -append -update -bandwidth 15 -m 50 -diff -i hdfs://192.168.190.89:8020/apps/hive/warehouse/csap.db/tb_si_cu_voma_limit_whitelist_day/statis_date=20170617 hdfs://172.19.168.4:8020/warehouse/tablespace/managed/hive/tb_si_cu_voma_limit_whitelist_day

远程集群
hdfs://192.168.190.88:8020/apps/hive/warehouse/csap.db/tb_si_cu_voma_limit_whitelist_day/statis_date=20170617
hdfs://192.168.190.88:8020/apps/hive/warehouse/csap.db/tb_si_cu_voma_limit_whitelist_day/statis_date=20170617

hadoop fs -du -h hdfs://192.168.190.89:8020/apps/hive/warehouse/csap.db/tb_si_cu_voma_limit_whitelist_day/
hadoop fs -du -h hdfs://192.168.190.89:8020/apps/hive/warehouse/csap.db/tb_si_cu_voma_limit_whitelist_day/statis_date=20170617

hadoop distcp -append -update -bandwidth 15 -m 50 -diff -i -log ./ hdfs://192.168.190.89:8020/apps/hive/warehouse/csap.db/tb_si_cu_voma_limit_whitelist_day/statis_date=20170617 hdfs://172.19.168.4:8020/warehouse/tablespace/managed/hive/tb_si_cu_voma_limit_whitelist_day

hadoop fs -du -h hdfs://192.168.190.89:8020/apps/hive/warehouse/csap.db/tb_si_cu_voma_limit_whitelist_day | more

hadoop fs -du -h hdfs://192.168.190.89:8020/apps/hive/warehouse/csap.db/tb_si_cu_voma_limit_whitelist_day/statis_date=20170617
