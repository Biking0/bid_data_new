#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：copy_hdfs_data.py
# 功能描述：迁移Hive表
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20200618
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python copy_hdfs_data.py
# ***************************************************************************


import os
import sys

# 旧集群
old_hdfs = 'hdfs://192.168.190.89:8020/apps/hive/warehouse/csap.db'

# 新集群
new_hdfs = 'hdfs://172.19.168.4:8020/warehouse/tablespace/managed/hive'

# 拷贝命令
# hadoop distcp -i hdfs://192.168.190.89:8020/apps/hive/warehouse/csap.db/tb_si_cu_voma_limit_whitelist_day/statis_date=20170617 hdfs://172.19.168.4:8020/warehouse/tablespace/managed/hive/tb_si_cu_voma_limit_whitelist_day

