#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：main.py
# 功能描述：主程序
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20200819
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python main.py
# ***************************************************************************

import os
import sys
import datetime as date_time

# 常量定义


# 连接新集群
new_hive = "beeline -u 'jdbc:hive2://172.19.168.101:10000/csap' -n ocdp -p 1q2w1q@W -e \" "


def read_table_name():
    f = open('./test_table_name.txt', 'r')
    for line in f.readlines():
        line = line.strip('\n')

        print ' #########################'
        print line
        copy_data(line)

        # 连续读表
        # break

    print '无任务'


# 重命名表
def rename_table(table_name):
    new_table_name = table_name + "_bakup"

    rename_sql = "ALTER TABLE " + table_name + " RENAME TO " + new_table_name + ";"

    rename_sql_sh = new_hive + rename_sql + "\""

    print 'rename_sql_sh', rename_sql_sh

    result = os.system(rename_sql_sh)

    if result != 0:
        # 失败
        print '### error'
        error_file = open("./rename_error.txt", 'a+')
        now_time = date_time.datetime.now()
        error_file.write(str(now_time) + " " + table_name+"\n")

    else:
        # 成功
        ok_file = open("./rename_ok_file.txt", 'a+')
        now_time = date_time.datetime.now()
        ok_file.write(str(now_time) + " " + table_name+"\n")


# 建新表
def create_table_like(table_name):
    new_table_name = table_name + "_bakup"
    create_table_like_sql = "create table " + table_name + " like " + new_table_name

    create_table_like_sql_sh = new_hive + create_table_like_sql + '\"'

    print 'create_table_like_sql_sh', create_table_like_sql_sh

    result = os.system(create_table_like_sql_sh)

    if result != 0:
        # 失败
        print '### error'
        error_file = open("./create_error.txt", 'a+')
        now_time = date_time.datetime.now()
        error_file.write(str(now_time) + " " + table_name+"\n")

    else:
        # 成功
        ok_file = open("./create_ok_file.txt", 'a+')
        now_time = date_time.datetime.now()
        ok_file.write(str(now_time) + " " + table_name+"\n")


# 复制数据
def copy_data(table_name):
    new_table_name = table_name + "_bakup"
    mv_data_sh = "hadoop fs -cp   /warehouse/tablespace/managed/hive/csap.db/" + new_table_name + "/* /warehouse/tablespace/managed/hive/csap.db/" + table_name + "/"

    print 'mv_data_sh', mv_data_sh

    result = os.system(mv_data_sh)

    if result != 0:
        # 失败
        print '### error'
        error_file = open("./copy_error.txt", 'a+')
        now_time = date_time.datetime.now()
        error_file.write(str(now_time) + " " + table_name+"\n")

    else:
        # 成功
        ok_file = open("./copy_ok_file.txt", 'a+')
        now_time = date_time.datetime.now()
        ok_file.write(str(now_time) + " " + table_name+"\n")

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
