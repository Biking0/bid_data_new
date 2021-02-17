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

# 日期格式
day_format = '%Y%m%d'

# 连接mysql
mysql_sh = "mysql -h 172.19.168.22 -P 3308 -u zhao -pzhao zhao -e ' "
excute_hive_sh = "beeline -u 'jdbc:hive2://hua-dlzx2-a0202:10000/csap' -n ocdp -p 1q2w1q@W -e '"

# 同步状态

rename_status_0 = '0'
rename_status_1 = '1'
rename_status_2 = '2'
rename_status_3 = '3'

create_status_0 = '0'
create_status_1 = '1'
create_status_2 = '2'
create_status_3 = '3'

insert_status_0 = '0'
insert_status_1 = '1'
insert_status_2 = '2'
insert_status_3 = '3'
