#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：copy_data.py
# 功能描述：导出表结构到文件
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20200819
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python copy_data.py
# ***************************************************************************

import os
import sys

# 常量定义

# 连接旧集群
old_hive = "beeline -u 'jdbc:hive2://192.168.190.88:10000/csap' -e 'show create table "

# 连接新集群
new_hive = "beeline -u 'jdbc:hive2://172.19.168.101:10000/csap' -n ocdp -p 1q2w1q@W -e \" "


def read_table_name():
    f = open('./test_table_name.txt', 'r')
    i = 1
    for line in f.readlines():
        line = line.strip('\n')

        print 1, ' #########################'
        print line
        copy_data(line)

        # 连续读表
        # break


def copy_data(table_name):
    new_table_name = table_name + "_bakup"
    mv_data_sh = "hadoop fs -cp   /warehouse/tablespace/managed/hive/csap.db/" + new_table_name + "/* /warehouse/tablespace/managed/hive/csap.db/" + table_name + "/"

    print 'mv_data_sh', mv_data_sh

    result = os.system(mv_data_sh)

    if result != 0:
        # 失败
        print '### error'
        error_file = open("./error.txt", 'a+')
        error_file.write(table_name)

    else:
        # 成功
        ok_file = open("./ok_file.txt", 'a+')
        ok_file.write(table_name)

    repair_table(table_name)


# 修复表
def repair_table(table_name):
    repair_table_sql = "msck repair table " + table_name

    add_partition_sql_sh = new_hive + repair_table_sql + "\""

    os.popen(add_partition_sql_sh)


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
