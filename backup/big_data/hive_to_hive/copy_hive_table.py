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


# 常量定义

# 连接旧集群
old_hive = "beeline -u 'jdbc:hive2://192.168.190.88:10000/csap' -e 'show create table "

# 连接新集群
new_hive = "beeline -u 'jdbc:hive2://172.19.168.101:10000/csap' -n ocdp -p 1q2w1q@W -e \" "


# 获取表结构，封装完整hive创建语句
def get_table_struct(table_name):
    table_struct_sh = old_hive + table_name + '\' > test1.txt'

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
        '\'\'', '\'|\'').replace('\n', '')

    # 去除LOCATION
    local_localtion = data.find('LOCATION')

    # print local_localtion
    result = data[0:local_localtion]

    # 去除多余空格
    result = ' '.join(result.split())

    # create_table_sql_sh = 'echo ' + result + ' ; >> test_create_table.txt\n'
    #
    # os.popen(create_table_sql_sh)

    create_table_sql = open('./create_table.txt', 'a+')
    create_table_sql.write(result + ' ;\n')
    create_table(result)


# 新集群建表操作
def create_table(create_table_sql):
    create_table_sh = new_hive + create_table_sql + '\"'

    print create_table_sh

    # 执行建表语句
    # create_result = os.popen(create_table_sh).readlines()

    print '### create_result'
    # print create_result


# 导出sql到文件
def export_sql():
    # print 123
    pass


def read_table_name():
    f = open('./test_table_name.txt', 'r')
    i = 1
    for line in f.readlines():
        line = line.strip('\n')

        print 1, ' #########################'
        print line
        get_table_struct(line)

        # 连续读表
        # break


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
