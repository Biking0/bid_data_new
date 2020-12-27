#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：export_create_table.py
# 功能描述：hive表数据稽核
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20200624
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python export_create_table.py
# ***************************************************************************

# 1. 分析hive库表结构，获取int字段，将所有表存到列表里
# 2. 构造数据稽核sql，分析周期（分区），sum字段
# 3. 抽取两个库的表文件到本地，进行对比

import os
import sys
import time
import datetime
import hive_to_hive.export_create_table_sql.config
import hive_to_hive.export_create_table_sql.pubUtil
import threading
from Queue import Queue

# 生产环境
# excute_desc_sh = "beeline -u 'jdbc:hive2://192.168.190.88:10000/csap' -n hive -p %Usbr7mx -e "
excute_desc_sh = "beeline -u 'jdbc:hive2://hua-dlzx2-a0202:10000/csap' -n ocdp -p 1q2w1q@W -e "


# 测试环境
# excute_desc_sh = "beeline -u 'jdbc:hive2://172.22.248.19:10000/default' -n csap -p @WSX2wsx -e "


# 生成desc表结构文件
def create_desc(table_name, result_queque):
    # 生产环境
    desc_sh = "beeline -u 'jdbc:hive2://hua-dlzx2-a0202:10000/csap' -n ocdp -p 1q2w1q@W -e 'desc  " + table_name + ' \' >> /home/ocdp/hyn/export_create_table/' + table_name + '.txt'

    # 测试环境
    # desc_sh = "beeline -u 'jdbc:hive2://172.22.248.19:10000/default' -n csap -p @WSX2wsx -e 'desc  " + table_name + ' \' > ./' + table_name + '.txt'

    print desc_sh
    os.popen(desc_sh).readlines()
    desc_parser(table_name, result_queque)


# 解析desc表结构
def desc_parser(table_name, result_queque):
    desc_list = open('/home/ocdp/hyn/export_create_table/' + table_name + '.txt', 'r').readlines()

    result_list = []

    for i in range(len(desc_list)):

        # 忽略其他行
        if desc_list[i][0] == '+':
            continue
        line_list = desc_list[i].strip().replace(' ', '').replace('\t', '').replace('\n', '').split('|')

        # 忽略表头
        if line_list[1] == 'col_name' or 'NULL' in line_list[1]:
            continue

        # break

    # 封装表结构int字段
    print 'int colume:'
    print result_list

    # 分区检测
    check_partition(table_name, result_list, result_queque)


# 分区检测，构造分区，根据需要稽核的时间段，循环生成相应的分区，判断是否为分区表,line(table_name)
def check_partition(line, result_list, result_queque):
    desc_list = open('/home/ocdp/hyn/export_create_table/' + line + '.txt', 'r').readlines()

    # result_list = []
    partition_list = []

    for i in range(len(desc_list)):

        # 忽略其他行
        if desc_list[i][0] == '+':
            continue
        line_list = desc_list[i].strip().replace(' ', '').replace('\t', '').replace('\n', '').split('|')

        # 检测外部表
        if 'EXTERNAL' in line_list[1]:
            result_queque.put(line)
            print '### find EXTERNAL'
            break

        # 忽略表头
        if line_list[1] == 'col_name' or 'NULL' in line_list[1]:
            continue

        if 'Partition' not in line_list[1]:
            print line_list[1], line_list[2], line_list[3],
            print '#'

    # 创建查询sql
    # create_sql(line, result_list, partition)


# 读取表名
def read_table_name():
    f = open('/home/ocdp/hyn/export_create_table/test_table_name.txt', 'r')
    i = 1

    multi_list = []

    for line in f.readlines():
        line = line.strip('\n')

        print 1, ' #########################'
        print line
        multi_list.append(line)

        # 开始解析
        # create_desc(line)

        # 连续读取目标表
        # break

    multi_thread(multi_list)


# 遍历列表
def read_list(num, data_queque, result_queque):
    for i in range(data_queque.qsize()):
        try:
            if not data_queque.empty():
                # 出队列
                table_name = data_queque.get()

                print 'table_name', table_name
                create_desc(table_name, result_queque)

        except Exception as e:
            print e
            continue


# 多线程
def multi_thread(multi_list):
    print 'multi_list', multi_list

    print '1', multi_list[0:2]
    print '2', list(multi_list[0:2])

    data_queque = Queue()
    result_queque = Queue()

    # 数据放入队列
    for i in range(len(multi_list)):
        data_queque.put(multi_list[i])

    # 设置并发数
    a = 5
    for i in range(a):
        # list分块，调用多线程
        multi1 = threading.Thread(target=read_list, args=(5, data_queque, result_queque))

        multi1.start()

    # 将结果写入文件
    export_to_file(result_queque)

# 导出结果到文件
def export_to_file(result_queque):
    f = open('../result.txt', 'a+')
    for i in range(result_queque.qsize()):
        table_name = result_queque.get()
        f.write(table_name)

    f.close()


# 运行之前清理结果表分区，添加重跑功能
def clear_ocdp_partition():
    # 清理ocdp集群分区
    sql = "alter table chk_result drop if exists partition(static_date=" + hive_to_hive.export_create_table_sql.pubUtil.get_today() + ");"

    clear_sql_sh = hive_to_hive.export_create_table_sql.config.excute_ocdp_sh + sql + '\''

    print clear_sql_sh

    # os.popen(clear_sql_sh)


read_table_name()
