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
# 程序调用格式：nohup python vt_data_check.py > nohup.out &
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
import vt_conn_db
import mysql_conn_db

excute_desc_sh = "hive -e"
partition_date = '20200828'


def main(table_name):
    try:
        table_name_list = table_name.split('.')
        database = table_name_list[0]
        table_name = table_name_list[1]

        # 表不存在
        if check_table(table_name, database):
            return

        end_string = get_end_string(table_name, database)
        table_int_list = get_int(table_name, database)
        partition = check_partition(table_name, partition_date, database)
        create_sql(table_name, table_int_list, partition, end_string, database)
    except Exception as e:
        print e
        print '异常'


# 检测表是否存在
def check_table(table_name, database):
    check_table_sql = "select  * from columns where  table_name=\'" + table_name + '\''

    print 'check_table_sql:', check_table_sql
    result = vt_conn_db.select(check_table_sql, database)

    # print 'check_table:', result
    # 表不存在
    if len(result) == 0:
        return True

    # 表存在
    else:
        return False


# 判断是否为分区表，
def check_partition(table_name, partition_date, database):
    partition_str = "statis_date"

    get_partition_sql = "select  column_name from columns where column_name='" + partition_str + "'   and table_name=\'" + table_name + '\''

    print 'get_end_string_sql:', get_partition_sql
    result = vt_conn_db.select(get_partition_sql, database)

    # 无分区
    if len(result) == 0:
        return ''

    # 有分区
    else:
        return partition_str + '=\'' + partition_date + '\''


# 获取最后一个字符串
def get_end_string(table_name, database):
    get_end_string_sql = "select a.column_name from columns a inner join ( select table_schema,table_name,max(column_id) mx_column_id from columns where data_type like 'varchar%'  group by 1,2) b on a.column_id = b.mx_column_id and a.table_name=\'" + table_name + '\''

    print 'get_end_string_sql:', get_end_string_sql
    result = vt_conn_db.select(get_end_string_sql, database)

    print result[0][0]
    return result[0][0]


# 获取int字段列表
def get_int(table_name, database):
    try:
        get_int_sql = "select  column_name from columns where  data_type ='int' and table_name=\'" + table_name + '\''
        print 'get_int_sql:', get_int_sql
        result = vt_conn_db.select(get_int_sql, database)
        print result

        return result
    except Exception as e:
        print 'int error:', e

        return 'int_error'


# 创建sql，进行查询,输入表名，int字段
def create_sql(table_name, table_int_list, partition, end_string, database):
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
        sql_part1 = "select 'DATA_SOURCE' as data_source,'" + database + '.' + table_name + "' as table_name,'" + partition + "', count(*)" + sql_part4
        sql_part3 = ",'REMARK',to_char(current_timestamp,'YYYY-MM-DD HH24:MI:SS') " + " from " + table_name + " ;"

    else:
        # select 'DATA_SOURCE',table_name,'partition',count(*),concat(nvl(sum(id),''),nvl(sum(name),'')),'REMARK',from_unixtime(unix_timestamp()) from table_name where patitions='';
        sql_part1 = "select 'DATA_SOURCE' as data_source,'" + database + '.' + table_name + "'  as table_name,'" + partition.replace(
            '\'', '') + "', count(*)" + sql_part4

        # todo 无分区表，增量数据无法稽核，全表可稽核
        sql_part3 = ",'REMARK',to_char(current_timestamp,'YYYY-MM-DD HH24:MI:SS') " + " from " + table_name + " where " + partition + ";"

    table_int_str = ''
    sql_part2 = ''
    # 无int字段
    if len(table_int_list) == 0:
        print "无int字段"
        table_int_str = "'no_int'"
        sql_part2 = ",%s" % (table_int_str)

    elif len(table_int_list) == 1:
        table_int_str = "sum(%s)" % table_int_list[0][0]
        sql_part2 = ",%s" % (table_int_str)
    else:
        for i in range(len(table_int_list)):
            table_int_str = table_int_str + "sum(%s)||'_'||" % (table_int_list[i][0])
            sql_part2 = ",%s" % (table_int_str[0:-7])

    print 'table_int_str', table_int_str

    sql = sql_part1 + sql_part2 + sql_part3

    print 'sql select :', sql

    # 执行查询
    select_sql_sh = excute_desc_sh + ' \" ' + sql + ' \"'
    print select_sql_sh
    # os.popen(select_sql_sh).readlines()

    insert_table(table_name, sql, database)

    # 删除表结构文本文件
    delete_sh = 'rm ' + table_name + '.txt'
    # os.popen(delete_sh).readlines()


# 构造出sql，将查询结果插入稽核结果表中
def insert_table(table_name, sql, database):
    # 随机插入1-10稽核结果表
    table_num = str(random.randint(1, 10))

    # chk_table_name = 'chk_result_' + table_num

    result = vt_conn_db.select(sql, database)
    print result

    check_table_name = config.check_table_name
    insert_sql = "insert into " + check_table_name + " (data_source,des_tbl,cyclical,count1,end_string_sum,sum1,remark,chk_dt,static_date) values('%s','%s','%s','%s','%s','%s','%s','%s','%s')" % (
        result[0][0], result[0][1], result[0][2], result[0][3], result[0][4], result[0][5], result[0][6], result[0][7],
        partition_date)

    print 'insert_sql', insert_sql

    mysql_conn_db.insert(insert_sql)


# 读取表名
def read_table_name():
    f = open('./test_table_name.txt', 'r')

    multi_list = []

    for line in f.readlines():
        line = line.strip('\n').replace('\t', '').replace(' ', '')

        print 1, ' #########################'
        print line
        multi_list.append(line)

        # 开始解析
        # main(line)

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
                main(table_name)
                # 随机休息10s
                time.sleep(random.randint(2, 5))

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
    a = 1
    # list分块，调用多线程
    for i in range(a):
        # list分块，调用多线程
        multi1 = threading.Thread(target=read_list, args=(5, data_queque, result_queque))

        multi1.start()


# 启动
if __name__ == '__main__':

    input_length = len(sys.argv)
    print 'input_str: ', len(sys.argv)

    if input_length == 2:
        global partition_date
        partition_date = sys.argv[1]
        print 'partition_date', partition_date
        read_table_name()

    else:
        print '输入参数有误'

# read_table_name()
# main("tb_dwd_ct_ngcs_teamworkcall_staffs_day")
# main("tb_dwd_ct_85ct_call_list_hour")
