# !/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：check_error.py
# 功能描述：检查迁移报错
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20200816
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：nohup python check_error.py >> nohup.out &
# ***************************************************************************

# 检查迁移报错，若分区不存在，将日志表拷贝状态改为2，remark2填分区不存在

import os
import sys
import conn_db

sy_hdfs_path = "hadoop fs -ls hdfs://192.168.190.89:8020/apps/hive/warehouse/csap.db/"


def get_error_log():
    try:
        get_error_sql = "select table_name,partition_type,partition_time,copy_status from tb_copy_data_log where copy_status='3'"

        print 'get_error_sql', get_error_sql
        get_error_result = conn_db.select(get_error_sql)
        for i in get_error_result:
            table_name, partition_type, partition_time, copy_status = i[0], i[1], i[2], i[3]
            check_partition(table_name, partition_type, partition_time, copy_status)
    except Exception as e:
        print e
        print '异常'


def check_partition(table_name, partition_type, partition_time, copy_status):
    print 'get result:', table_name, partition_type, partition_time, copy_status

    check_sh = sy_hdfs_path + table_name + "/" + partition_type + "=" + partition_time

    check_result = os.system(check_sh)

    # 分区不存在
    if check_result != 0:
        update_mysql(table_name, partition_time)

    print 'check_sh:', check_sh


def update_mysql(table_name, partition_time):
    update_sql = "update tb_copy_data_log set copy_status ='2' ,remark2='partition not exist' where table_name='" + table_name + "' and partition_time='" + partition_time + "\'"

    conn_db.insert(update_sql)

    print 'update_sql', update_sql
