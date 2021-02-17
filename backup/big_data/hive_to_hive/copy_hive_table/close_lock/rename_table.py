#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：rename_table.py
# 功能描述：重命名表
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20200617
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python rename_table.py
# ***************************************************************************

import os
import sys

# 常量定义

# 连接新集群
new_hive = "beeline -u 'jdbc:hive2://172.19.168.101:10000/csap' -n ocdp -p 1q2w1q@W -e \" "


def read_table_name():
    f = open('./test_table_name.txt', 'r')
    i = 1
    for line in f.readlines():
        line = line.strip('\n')

        print 1, ' #########################'
        print line
        rename_table(line)

        # 连续读表
        # break


# 重命名表
def rename_table(table_name):
    new_table_name = table_name + "_bakup"

    rename_sql = "ALTER TABLE " + table_name + " RENAME TO " + new_table_name + ";"

    rename_sql_sh = new_hive + rename_sql + "\""

    print 'rename_sql_sh', rename_sql_sh

    # result = os.system(rename_sql_sh)
    result = 0

    if result != 0:
        # 失败
        print '### error'
        error_file = open("./error.txt", 'a+')
        error_file.write(table_name)

    else:
        # 成功
        ok_file = open("./ok_file.txt", 'a+')
        ok_file.write(table_name)


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
