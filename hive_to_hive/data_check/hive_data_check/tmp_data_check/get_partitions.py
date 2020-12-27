#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：partition_ocr.py
# 功能描述：已筛选有分区表，获取所有分区
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20201217
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python partition_ocr.py
# ***************************************************************************

import os
import conn_db

table_file = open('./table_list.txt', 'r')


parttion_file = open('./pt_name.txt', 'a+')

table_list = table_file.readlines()
table_file.close()

# 遍历表名
for i in range(len(table_list)):
    table_name = table_list[i].replace('\n', '')
    sql = "select part_name from PARTITIONS where TBL_ID = ( select TBL_ID from TBLS where OWNER='hive' and tbl_name='%s' order by CREATE_TIME desc limit 1);" % (
        table_name)
    print 'sql:', sql

    result_list = conn_db.select(sql)

    for j in range(len(result_list)):
        name=result_list[j][0]
        print 'name',name
        parttion_file.write(table_name+' '+ name+'\n')


    # 测试
    # if i > 2:
    #     break


