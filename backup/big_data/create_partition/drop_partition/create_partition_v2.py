#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：create_partition.py
# 功能描述：python程序不会自己建分区
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20200617
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python create_partition.py

# ***************************************************************************

import os
import sys
from datetime import datetime
import datetime as date_time

day = 'statis_date'

new_hive = "beeline -u 'jdbc:hive2://172.19.40.241:10000/csap' -n ocdp -p 1q2w1q@W -e ' "

start_date = '20200825'
end_date = '20200930'
# 日期格式
day_format = '%Y%m%d'


def create_date():
    start_date_time = datetime.strptime(start_date, day_format)

    end_date_time = datetime.strptime(end_date, day_format)

    # 迁移周期跨度
    date_length = (end_date_time - start_date_time).days + 1

    # print '迁移周期：', date_length

    partition_date_init = start_date_time
    day_list = []
    # 遍历迁移周期
    for i in range(date_length):
        print i
        partition_date = str((partition_date_init + date_time.timedelta(days=i)).date()).replace('-', '')
        day_list.append(partition_date)

    # print day_list
    return day_list


def create_day_partion(table_name_list, day_list):
    add_partition = open('./add_partition.sql', 'a+')

    # 表名
    for i in range(len(table_name_list)):

        # 分区时间
        for j in range(len(day_list)):
            add_partion_sql = " alter table " + table_name_list[i] + " add if not exists partition(" + day + "=" + \
                              day_list[j] + ");"
            add_partion_sql_sh = new_hive + add_partion_sql
            # print add_partion_sql_sh

            add_partition.write(add_partion_sql + '\n')


def read_table_name():
    f = open('./test_table_name.txt', 'r')

    # day_list = ['']
    multi_list = []
    for line in f.readlines():
        line = line.strip('\n')

        # print 1, ' #########################'
        # print line
        # get_table_struct(line)
        multi_list.append(line)

    create_day_partion(multi_list, create_date())


# create_date()
read_table_name()
