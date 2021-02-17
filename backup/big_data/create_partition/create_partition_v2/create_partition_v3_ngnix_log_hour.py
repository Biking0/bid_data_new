#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：create_partition_v3_ngnix_log_hour.py
# 功能描述：python程序不会自己建分区
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20200617
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python create_partition_v3_ngnix_log_hour.py

# ***************************************************************************

import os
import sys
from datetime import datetime
import datetime as date_time

day = 'statis_date'

new_hive = "beeline -u 'jdbc:hive2://172.19.168.101:10000/csap' -n ocdp -p 1q2w1q@W -e \" "

start_date = '20200810'
end_date = '20200830'
# 日期格式
day_format = '%Y%m%d'

hour_list = ['00',
             '01',
             '02',
             '03',
             '04',
             '05',
             '06',
             '07',
             '08',
             '09',
             '10',
             '11',
             '12',
             '13',
             '14',
             '15',
             '16',
             '17',
             '18',
             '19',
             '20',
             '21',
             '22',
             '23']

topic_name = ['core_ha', 'core_ly', 'net_ha', 'net_ly', 'ngnix_ha', 'ngnix_ly']


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
            for m in hour_list:
                for n in topic_name:
                    add_partion_sql = " alter table " + table_name_list[
                        i] + " add if not exists partition(" + day + "=" + \
                                      day_list[j] + ",statis_hour=" + day_list[j] + m + ",topic_name='" + n + "');"
                    add_partion_sql_sh = new_hive + add_partion_sql + "\""
                    print add_partion_sql_sh

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
