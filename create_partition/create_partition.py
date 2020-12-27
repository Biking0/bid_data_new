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

day = 'statis_date'

new_hive = "beeline -u 'jdbc:hive2://172.19.168.101:10000/csap' -n ocdp -p 1q2w1q@W -e ' "

day_list = [
            '20200723'
            '20200724',
            '20200725',
            '20200726',
            '20200727',
            '20200728',
            '20200729',
            '20200730',
            '20200731',
            '20200801',
            '20200802',
            '20200803',
            '20200804',
            '20200805',
            '20200806'
            ]


def create_day_partion(table_name_list, day_list):
    add_partition = open('./add_partition.txt','a+')

    # 表名
    for i in range(len(table_name_list)):

        # 分区时间
        for j in range(len(day_list)):
            add_partion_sql = " alter table " + table_name_list[i] + " add if not exists partition(" + day + "=" + \
                              day_list[j] + ");"
            add_partion_sql_sh = new_hive + add_partion_sql
            print add_partion_sql_sh

            add_partition.write(add_partion_sql+'\n')


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

    create_day_partion(multi_list, day_list)


read_table_name()
