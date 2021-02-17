#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：config.py
# 功能描述：数据量稽核配置表
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20201014
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python config.py
# ***************************************************************************

partition_statis_date = 'statis_date'

# 迁移批次，0：日表，1：月表，2：年表，4：维表，全量
migration_batch = 0
int_0 = 0

# 日期格式
day_format = '%Y%m%d'

# 连接mysql
mysql_sh = "mysql -h 172.19.168.22 -P 3308 -u zhao -pzhao zhao -e ' "

# ocdp_hive_sh = "beeline -u 'jdbc:hive2://172.19.40.241:10000/csap' -n ocdp -p 1q2w1q@W -e '"
# ocdp_hive_sh = "beeline -u 'jdbc:hive2://172.19.40.241:10000/csap' -n ocdp -p 1q2w1q@W -e '"
sy_hive_sh = "hive -e ' use csap;"
ocdp_hive_sh = "hive -e ' use csap;"

# 稽核状态
check_status_0 = '0'
check_status_1 = '1'
check_status_2 = '2'

sy_db_name = 'old'
ocdp_db_name = 'new'
