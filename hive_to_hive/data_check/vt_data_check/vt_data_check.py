#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：vt_data_check.py
# 功能描述：vertica库数据稽核
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20200828
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python vt_data_check.py
# ***************************************************************************

# 1. 分析hive库表结构，获取int字段，将所有表存到列表里
# 2. 构造数据稽核sql，分析周期（分区），sum字段
# 3. 抽取两个库的表文件到本地，进行对比

import os
import sys
import time
import datetime
import config
import random
import threading
from Queue import Queue
import conn_db

excute_desc_sh = "hive -e"


def main(table_name):
    partition_date = '20200828'
    end_string = get_end_string(table_name)
    table_int_list = get_int(table_name)
    partition = check_partition(table_name, partition_date)
    create_sql(table_name, table_int_list, partition, end_string)


# 判断是否为分区表，
def check_partition(table_name, partition_date):
    partition_str = "statis_date"

    get_partition_sql = "select  column_name from columns where column_name='" + partition_str + "'   data_type ='int' and table_name=\'" + table_name + '\''

    print 'get_end_string_sql:', get_partition_sql
    result = conn_db.select(get_partition_sql)

    # 无分区
    if len(result[0]) == 0:
        return ''

    # 有分区
    else:
        return partition_date


# 获取最后一个字符串
def get_end_string(table_name):
    get_end_string_sql = "select a.column_name from columns a inner join ( select table_schema,table_name,max(column_id) mx_column_id from columns where data_type like 'varchar%'  group by 1,2) b on a.column_id = b.mx_column_id and a.table_name=\'" + table_name + '\''

    print 'get_end_string_sql:', get_end_string_sql
    result = conn_db.select(get_end_string_sql)

    print result[0][0]
    return result[0][0]


# 获取int字段列表
def get_int(table_name):
    get_int_sql = "select  column_name from columns where  data_type ='int' and table_name=\'" + table_name + '\''
    print 'get_int_sql:', get_int_sql
    result = conn_db.select(get_int_sql)

    print result[0]
    return result[0]


# 创建sql，进行查询,输入表名，int字段
def create_sql(table_name, table_int_list, partition, end_string):
    sql_part1 = ''
    sql_part3 = ''

    # end_string为空，该表无string类型字段
    if end_string == '':
        sql_part4 = ",'no_string_col'"
    else:
        sql_part4 = ",sum(length(" + end_string + "))"

    # 无分区
    if partition == '':
        partition = 'no_partition'
        sql_part1 = "select 'DATA_SOURCE','" + table_name + "','" + partition + "', count(*)" + sql_part4
        sql_part3 = ",'REMARK',from_unixtime(unix_timestamp()) " + " from " + table_name + " ;"

    else:
        # select 'DATA_SOURCE',table_name,'partition',count(*),concat(nvl(sum(id),''),nvl(sum(name),'')),'REMARK',from_unixtime(unix_timestamp()) from table_name where patitions='';
        sql_part1 = "select 'DATA_SOURCE','" + table_name + "','" + partition + "', count(*)" + sql_part4

        # todo 无分区表，增量数据无法稽核，全表可稽核
        sql_part3 = ",'REMARK',from_unixtime(unix_timestamp()) " + " from " + table_name + " where " + partition + ";"

    table_int_str = ''
    for i in range(len(table_int_list)):
        table_int_str = table_int_str + "sum(%s)||'_'||" % (table_int_list[i])

    print 'table_int_str', table_int_str

    sql_part2 = ",concat(%s)" % (table_int_str[0:-5])

    sql = sql_part1 + sql_part2 + sql_part3

    print 'sql select :', sql

    # 执行查询
    select_sql_sh = excute_desc_sh + ' \" ' + sql + ' \"'
    print select_sql_sh
    # os.popen(select_sql_sh).readlines()

    # insert_table(table_name, sql)

    # 删除表结构文本文件
    delete_sh = 'rm ' + table_name + '.txt'
    # os.popen(delete_sh).readlines()


# 构造出sql，将查询结果插入稽核结果表中
def insert_table(table_name, sql):
    # 随机插入1-10稽核结果表
    table_num = str(random.randint(1, 10))

    chk_table_name = 'chk_result_' + table_num

    insert_sql = " use csap; insert into table " + chk_table_name + " partition (static_date=" + time.strftime(
        "%Y%m%d",
        time.localtime(
            time.time())) + ") " + sql
    print insert_sql

    # 执行插入语句
    insert_sql_sh = excute_desc_sh + ' \" ' + insert_sql + ' \" '
    print insert_sql_sh
    os.popen(insert_sql_sh).readlines()

    # 导出数据到文件
    # export_chk_result(table_name)

    # 苏研数据迁移到ocdp集群
    # distcp_sy_to_ocdp()


# 导出稽核结果表到文件
def export_chk_result(table_name):
    export_sql = "use csap; select DES_TBL,CYCLICAL,COUNT1,SUM1,REMARK from %s;" % (table_name)

    export_sh = excute_desc_sh + ' \" ' + export_sql + ' \" ' + ' >> %s.txt' % (table_name)

    print 'export_sh', export_sh

    os.popen(export_sh).readlines()


# 读取表名
def read_table_name():
    f = open('/home/hive/hyn/data_check/test_table_name.txt', 'r')
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


# 遍历列表
def read_list(num, data_queque, result_queque):
    for i in range(data_queque.qsize()):
        try:
            if not data_queque.empty():
                # 出队列
                table_name = data_queque.get()

                # print 'table_name', table_name
                # create_desc(table_name)

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
    a = 40
    # list分块，调用多线程
    for i in range(a):
        # list分块，调用多线程
        multi1 = threading.Thread(target=read_list, args=(5, data_queque, result_queque))

        multi1.start()


# read_table_name()
get_end_string("tb_dwd_ct_ngcs_teamworkcall_staffs_day")
