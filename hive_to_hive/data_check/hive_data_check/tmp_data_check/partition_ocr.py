#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：partition_ocr.py
# 功能描述：过滤出分区表
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20201028
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python copy_hive_table.py
# ***************************************************************************



import os
import conn_db

table_file = open('./table_list.txt', 'r')
no_file = open('./no_list.txt', 'a+')
yes_file = open('./yes_count_list.txt', 'a+')
table_list = table_file.readlines()
table_file.close()

# 遍历表名
for i in range(len(table_list)):
    table_name = table_list[i].replace('\n', '')
    sql = "select count(*) from PARTITIONS where TBL_ID = ( select TBL_ID from TBLS where OWNER='hive' and tbl_name='%s' order by CREATE_TIME desc limit 1);" % (
        table_name)
    print 'sql:', sql

    result = conn_db.select(sql)

    print result
    print result[0][0]

    pt_count = int(result[0][0])

    if pt_count == 0:
        print '非分区表', table_name
        no_file.write(table_list[i])
    else:
        print '分区表', table_name
        yes_file.write(table_name + ' ' + str(pt_count) + '\n')

    # 测试
    # if i > 10:
    #     break

no_file.close()
yes_file.close()
