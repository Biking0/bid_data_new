#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：ocdp_export_show_create.py
# 功能描述：迁移Hive表
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20200617
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python ocdp_export_show_create.py

# ***************************************************************************

import os
import sys
import datetime
import threading
from Queue import Queue

# 常量定义

# 连接旧集群
old_hive = "beeline -u 'jdbc:hive2://192.168.190.88:10000/csap' -e 'show create table "

# 连接新集群
# new_hive = "beeline -u 'jdbc:hive2://172.19.168.101:10000/csap' -n ocdp -p 1q2w1q@W -e \" "

new_hive = "beeline -u 'jdbc:hive2://172.19.168.101:10000/csap' -n ocdp -p 1q2w1q@W -e 'show create table "

cluster = 'ocdp'

# 获取表结构，封装完整hive创建语句
def get_table_struct(table_name):
    # table_struct_sh = old_hive + table_name + '\' > test1.txt'
    table_struct_sh = new_hive + table_name + '\' > test1.txt'

    table_struct_str = os.popen(table_struct_sh).readlines()
    f = open('table_struct.txt', 'w')
    f.write(str(table_struct_str))
    f.close()

    # print table_struct_sh
    # print table_struct_str

    data = ''
    with open("./test1.txt", "r") as f:  # 打开文件
        data = f.read()  # 读取文件
        if 'LOCATION' in data:
            print(data)

    print '###########'

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

    # 去除LOCATION
    # local_localtion = data.find('LOCATION')
    # 表结构稽核
    local_localtion = data.find('ROW')

    # print local_localtion
    result = data[0:local_localtion]

    # 去除多余空格
    result = ' '.join(result.split())

    # create_table_sql_sh = 'echo ' + result + ' ; >> test_create_table.txt\n'
    #
    # os.popen(create_table_sql_sh)

    create_table_sql = open('./create_table.txt', 'a+')
    create_table_sql.write(result + ' ;\n')
    result = result + ' ;'
    # create_table(result)
    insert_mysql(table_name, result)


def insert_mysql(table_name, result):
    today = str(datetime.date.today()).replace('-', '')
    insert_sql_sh = "mysql -h 172.19.168.22 -P 3308 -u zhao -pzhao zhao -e 'insert into check_table_struct (cluster,table_name,create_table_sql,check_time) values(\"" + cluster + "\",\""+table_name+"\",\"" + result + "\",\"" + today + "\")'"
    print 'insert_sql_sh', insert_sql_sh
    insert_str = os.popen(insert_sql_sh).readlines()
    print 'insert_str', insert_str


# 新集群建表操作
def create_table(create_table_sql):
    create_table_sh = new_hive + create_table_sql + '\"'

    print create_table_sh

    # 执行建表语句
    create_result = os.popen(create_table_sh).readlines()

    # print '### create_result'
    # print create_result


def read_table_name():
    f = open('./test_table_name.txt', 'r')

    multi_list = []
    for line in f.readlines():
        line = line.strip('\n')

        # print 1, ' #########################'
        # print line
        # get_table_struct(line)
        multi_list.append(line)

        # 连续读表
        # break
    multi_thread(multi_list)


# 遍历列表
def read_list(num, data_queque, result_queque):
    for i in range(data_queque.qsize()):
        try:
            if not data_queque.empty():
                # 出队列
                table_name = data_queque.get()

                # print 'table_name', table_name
                get_table_struct(table_name)

        except Exception as e:
            print e
            f = open('./error_info.log', 'a+')
            f.write(str(e))
            f.close()
            continue


# 多线程
def multi_thread(multi_list):
    # print 'multi_list', multi_list

    data_queque = Queue()
    result_queque = Queue()

    # 数据放入队列
    for i in range(len(multi_list)):
        data_queque.put(multi_list[i])

    # 设置并发数
    a = 2
    # list分块，调用多线程
    for i in range(a):
        # list分块，调用多线程
        multi1 = threading.Thread(target=read_list, args=(5, data_queque, result_queque))

        multi1.start()


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
