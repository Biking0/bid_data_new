#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：config.py
# 功能描述：迁移Hive表
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20200810
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
excute_hive_sh = "beeline -u 'jdbc:hive2://172.19.40.241:10000/csap' -n ocdp -p 1q2w1q@W -e '"

# 同步状态

# 未同步
copy_status_0 = '0'
# 正在同步
copy_status_1 = '1'
# 同步完成
copy_status_2 = '2'
# 同步失败
copy_status_3 = '3'


# 稽核状态
# 未稽核
chk_status_0 = ''
# 数据源
data_source = 'sy'

# 全量表
all_table = 'all'

# 带宽限制参数
bandwidth = '30'
map_num = '50'
