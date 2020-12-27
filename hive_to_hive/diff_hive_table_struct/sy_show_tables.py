#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：sy_show_tables.py
# 功能描述：迁移Hive表
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20200725
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python sy_show_tables.py
# ***************************************************************************

import os
import sys

# 功能
# 1.show_tables到列表变量
# 2.过滤符号排序，输出到文件

# 连接ocdp集群
excute_ocdp_sh = "hive -e "


def show_tables():
    show_tables_sql = 'use csap;show tables;'
    show_tables_sh = excute_ocdp_sh + '\"' + show_tables_sql + '\"'

    print show_tables_sh
    show_tables_list = os.popen(show_tables_sh).readlines()
    # print show_tables_list

    result_list = []
    for i in range(len(show_tables_list)):

        show_tables_list[i] = show_tables_list[i].replace('\n', '').replace(' ', '').replace('|', '')
        if ('+' in show_tables_list[i]) or ('tab_name' in show_tables_list[i]):
            continue
        result_list.append(show_tables_list[i])

    # result_list=result_list.sorted()

    result_list = sorted(result_list)
    ocdp_table_name = open('./sy_table_name.txt', 'w')
    for i in range(len(result_list)):
        ocdp_table_name.write(result_list[i] + '\n')


show_tables()
