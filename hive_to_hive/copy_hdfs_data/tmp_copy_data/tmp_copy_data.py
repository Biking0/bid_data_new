# !/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：tmp_copy_data.py
# 功能描述：检查迁移报错
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20200925
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：nohup python tmp_copy_data.py >> nohup.out &
# ***************************************************************************

# 检查迁移报错，若分区不存在，将日志表拷贝状态改为2，remark2填分区不存在

import os
import sys
import time
import conn_db

sy_hdfs_path = "hadoop fs -ls hdfs://192.168.190.89:8020/apps/hive/warehouse/csap.db/"


def get_time():
    try:
        get_time_str_sql = "select time_str from tb_tmp_copy_data "

        result = conn_db.select(get_time_str_sql)[0][0]

        print 'get_time_str:', result

        return result

    except Exception as e:
        print e
        print '异常'


# 读取表名
def read_table_name():
    f = open('./test_table_name.txt', 'r')

    for line in f.readlines():
        # line = line.strip('\n').replace(' ', '').replace('\t', '')
        # line = line.strip('\n').replace(' ', '').replace('\t', '')

        print 1, ' #########################'
        print line

        os.system(line)

        # 休息10s
        time_str = int(get_time())
        print 'sleep:', time_str
        time.sleep(time_str)

        # 开始解析
        # create_desc(line)

        # 连续读取目标表
        # break
    print '无任务'


read_table_name()
