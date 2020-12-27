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
# 程序调用格式：nohup python sy_count.py >> nohup.out &
# ***************************************************************************


import os
import sy_conn_db
import conn_local_db
import threading
from Queue import Queue

table_file = open('./table_list.txt', 'r')
no_file = open('./no_list.txt', 'a+')
yes_file = open('./yes_count_list.txt', 'a+')
table_list = table_file.readlines()
table_file.close()


def sy_count(table_name):
    sql = "select part_id,part_name from PARTITIONS where TBL_ID = ( select TBL_ID from TBLS where OWNER='hive' and tbl_name='%s' order by CREATE_TIME desc limit 1);" % (
        table_name)
    print 'sql:', sql

    result_list = sy_conn_db.select(sql)

    # print 'result_list', result_list

    insert_sql_list = []

    # 遍历分区
    for j in range(len(result_list)):
        part_id = result_list[j][0]
        part_name = result_list[j][1]
        # print 'part_id:', part_id

        sql_count = "select param_value from  partition_params where part_id='%s' and param_key='numRows' " % (part_id)

        count_result = sy_conn_db.select(sql_count)[0][0]

        # print 'count_result:', count_result

        # yes_file.write(table_name + ' ' + part_name + ' ' + str(count_result) + '\n')

        insert_sql = "insert into tb_tmp_check_data (table_name,part_name,sy_count) values ('%s','%s','%s')" % (
            table_name, part_name, count_result)

        insert_sql_list.append(insert_sql)

    # 插入数据库
    conn_local_db.insert_batch(insert_sql_list)

    insert_sql_list = []


# 遍历列表
def read_list(num, data_queque, result_queque):
    for i in range(data_queque.qsize()):
        try:
            if not data_queque.empty():
                # 出队列
                table_name = data_queque.get()

                # print 'table_name', table_name
                sy_count(table_name)

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
    a = 5
    # list分块，调用多线程
    for i in range(a):
        # list分块，调用多线程
        multi1 = threading.Thread(target=read_list, args=(5, data_queque, result_queque))

        multi1.start()


# 读取表名
def read_table_name():
    f = open('./table_list.txt', 'r')
    i = 1

    multi_list = []

    for line in f.readlines():
        line = line.strip('\n').replace('\t', '').replace(' ', '')

        print 1, ' #########################'
        print line
        multi_list.append(line)

        # 开始解析
        # create_desc(line)

        # 连续读取目标表
        # break

    multi_thread(multi_list)


read_table_name()
