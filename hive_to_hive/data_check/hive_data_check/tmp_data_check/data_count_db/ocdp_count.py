#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：sy_count.py
# 功能描述：过滤出分区表
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20201028
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python sy_count.py
# ***************************************************************************


import os
import ocdp_conn_db
import conn_local_db

table_file = open('./table_list.txt', 'r')
no_file = open('./no_list.txt', 'a+')
yes_file = open('./yes_count_list.txt', 'a+')
table_list = table_file.readlines()
table_file.close()

# 遍历表名
for i in range(len(table_list)):
    table_name = table_list[i].replace('\n', '')
    sql = "select part_id,part_name from PARTITIONS where TBL_ID = ( select TBL_ID from TBLS where OWNER='hive' and tbl_name='%s' order by CREATE_TIME desc limit 1);" % (
        table_name)
    print 'sql:', sql

    result_list = ocdp_conn_db.select(sql)

    # print 'result_list', result_list

    insert_sql_list = []

    # 遍历分区
    for j in range(len(result_list)):
        part_id = result_list[j][0]
        part_name = result_list[j][1]
        # print 'part_id:', part_id

        sql_count = "select param_value from  partition_params where part_id='%s' and param_key='numRows' " % (part_id)

        count_result = ocdp_conn_db.select(sql_count)[0][0]

        # print 'count_result:', count_result

        # yes_file.write(table_name + ' ' + part_name + ' ' + str(count_result) + '\n')

        insert_sql = "insert into tb_tmp_check_data (table_name,part_name,sy_count) values ('%s','%s','%s')" % (
        table_name, part_name, count_result)

        insert_sql_list.append(insert_sql)

    # 插入数据库
    conn_local_db.insert_batch(insert_sql_list)

    insert_sql_list=[]

    # print result
    # print result[0][0]
    #
    # pt_count = int(result[0][0])
    #
    # if pt_count == 0:
    #     print '非分区表', table_name
    #     no_file.write(table_list[i])
    # else:
    #     print '分区表', table_name
    #     yes_file.write(table_name + ' ' + str(pt_count) + '\n')

    # 遍历分区

    # 测试
    # if i > 10:
    #     break

no_file.close()
yes_file.close()
