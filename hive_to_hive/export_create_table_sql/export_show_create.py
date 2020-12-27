#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：copy_hive_table.py
# 功能描述：迁移Hive表
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20200617
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python copy_hive_table.py
# ***************************************************************************

import os
import sys
import threading
from Queue import Queue

# 常量定义

# 连接旧集群
old_hive = "beeline -u 'jdbc:hive2://192.168.190.88:10000/csap' -e 'show create table "

# 连接新集群
new_hive = "beeline -u 'jdbc:hive2://172.19.168.101:10000/csap' -n ocdp -p 1q2w1q@W -e 'show create table  "


# 获取表结构，封装完整hive创建语句
def get_table_struct(table_name, result_queque):
    table_struct_sh = new_hive + table_name + '\' > ./' + table_name + '.txt'

    table_struct_str = os.popen(table_struct_sh).readlines()

    with open('./' + table_name + '.txt', "r") as f:  # 打开文件
        data = f.read()  # 读取文件
        if 'LOCATION' in data:
            print(data)

    print '###########'

    print 'data',data
    # 过滤建表语句
    # data = data.replace('+', '').replace('-', '').replace('+', '').replace('createtab_stmt', '').replace('|',
    #                                                                                                      '').replace(
    #     '\'\'', '\'|\'').replace('\n', '').replace('`', '').replace(' _c0 ', ' `_c0` ').replace(' _c1 ', ' `_c1` ').replace(' _c2 ',
    #                                                                                             ' `_c2` ').replace(
    #     ' _c3 ', ' `_c3` ').replace(' _c4 ', ' `_c4` ').replace(' _c5  ', ' `_c5 ` ').replace(' _c6  ',
    #                                                                                           ' `_c6 ` ').replace(
    #     ' _c7  ', ' `_c7 ` ').replace(' _c8  ', ' `_c8 ` ').replace(' _c9 ', ' `_c9 ` ').replace(' _c10 ',
    #                                                                                              ' `_c10` ').replace(
    #     ' _c11 ', ' `_c11` ').replace(' _c12 ', ' `_c12` ').replace(' _c13 ', ' `_c13`').replace(' _c14 ',
    #                                                                                              ' `_c14` ').replace(
    #     ' _c15 ', ' `_c15` ')

    data = data.replace('+', '').replace('-', '').replace('+', '').replace('createtab_stmt', '').replace('|',
                                                                                                         '').replace(
        '\'\'', '\'|\'').replace('\n', '').replace('`', '')

    # 检测外部表
    if data.find('EXTERNAL'):
        result_queque.put(table_name)
        print 'find table name ', table_name
        print '### find EXTERNAL'
        return


# 遍历列表
def read_list(num, data_queque, result_queque):
    for i in range(data_queque.qsize()):
        try:
            if not data_queque.empty():
                # 出队列
                table_name = data_queque.get()

                print 'table_name', table_name
                get_table_struct(table_name, result_queque)

        except Exception as e:
            print e
            continue


# 多线程
def multi_thread(multi_list):


    data_queque = Queue()
    result_queque = Queue()

    # 数据放入队列
    for i in range(len(multi_list)):
        data_queque.put(multi_list[i])

    # 设置并发数
    a = 1
    for i in range(a):
        # list分块，调用多线程
        multi1 = threading.Thread(target=read_list, args=(5, data_queque, result_queque))

        multi1.start()

    # 将结果写入文件
    export_to_file(result_queque)


# 导出结果到文件
def export_to_file(result_queque):
    f = open('./result.txt', 'a+')
    for i in range(result_queque.qsize()):
        table_name = result_queque.get()
        f.write(table_name)

    f.close()


def read_table_name():
    f = open('./test_table_name.txt', 'r')
    i = 1
    multi_list = []

    for line in f.readlines():
        line = line.strip('\n')

        print 1, ' #########################'
        print line

        multi_list.append(line)

        # break

    multi_thread(multi_list)


# 启动入口
if __name__ == '__main__':
    read_table_name()
    # # 输入表明参数处理
    # input_length = len(sys.argv)
    # print 'input_str: ', len(sys.argv)
    #
    # monitor_server = 1
    # if input_length == 2:
    #     get_table_struct(sys.argv[1])
    #
    # else:
    #     print '输入表名参数'
