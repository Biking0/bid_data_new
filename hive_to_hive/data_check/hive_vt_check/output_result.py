#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：output_result.py
# 功能描述：汇总稽核结果
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20210502
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python output_result.py
# ***************************************************************************

# 1.从mysql库获取稽核表清单
# 2.关联汇总稽核结果

import os
import random
import time
import datetime
import config
# import pubUtil
import threading
from Queue import Queue
import mysql_conn_db
import traceback


class Output_result():

    def __init__(self):
        # chk_table_name = 'chk_result_' + table_num
        self.chk_table_name = 'tb_hive_check_result'

    # mysql读取表名
    def read_table_name(self):

        # 获取稽核目标表列表sql
        get_table_sql = "select table_name from tb_check_table_list where check_flag=1 "

        # 查询mysql获取稽核列表
        get_table_list = mysql_conn_db.select(get_table_sql)

        print get_table_list
        print len(get_table_list)
        print len(get_table_list[0])

        # 是否有稽核任务
        if len(get_table_list) == 0:
            print '无稽核任务'
        else:

            # 遍历稽核表清单，清理无效字符
            multi_list = []
            for line in get_table_list:
                line = line[0].strip('\n').replace(' ', '').replace('\t', '')

                print 1, ' #########################'
                print line
                multi_list.append(line)

                # 开始解析
                # self.create_desc(line)

                # 连续读取目标表
                # break

            self.multi_thread(multi_list)

    # 开启多线程
    def multi_thread(self, multi_list):
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
            multi1 = threading.Thread(target=self.read_list, args=(5, data_queque, result_queque))

            multi1.start()

    # 遍历列表
    def read_list(self, num, data_queque, result_queque):
        for i in range(data_queque.qsize()):
            try:
                if not data_queque.empty():
                    # 出队列
                    table_name = data_queque.get()

                    # print 'table_name', table_name
                    self.output_result(table_name)

            except Exception as e:

                print e
                print traceback.print_exc()

                # 记录稽核异常日志
                f = open('./error_info.log', 'a+')
                f.write(str(e) + '\n')
                f.write(str(datetime.datetime.now()) + '\n')
                f.close()
                continue

    # 汇总结果，vt表为准
    def output_result(self, table_name):

        data_time_sql = "select partition from tb_vt_check_result where table_name='%s' order by partition desc limit 1" % (
            table_name)

        print 'data_time_sql', data_time_sql

        data_time = mysql_conn_db.select(data_time_sql)[0][0]

        print 'data_time', data_time

        # check_flag，Vt与hive同名表稽核结果标记
        check_flag_sql = "select * from tb_hive_check_result a left join tb_vt_check_result b on a.table_name=b.table_name  where a.table_name ='%s' and a.partition='%s' and a.count_num=b.count_num and a.end_string_sum=b.end_string_sum and a.int_sum=b.int_sum " % (
            table_name, data_time)

        print 'check_flag_sql', check_flag_sql

        check_flag_result = mysql_conn_db.select(check_flag_sql)

        print 'check_flag_result', check_flag_result
        check_flag = 0
        if len(check_flag_result) > 0: check_flag = 1

        # check_flag，Vt与hive同名表稽核结果标记
        check_flag_chk_sql = "select * from tb_hive_chk_check_result a left join tb_vt_check_result b on a.table_name=b.table_name  where a.table_name ='%s' and a.partition='%s' and a.count_num=b.count_num and a.end_string_sum=b.end_string_sum and a.int_sum=b.int_sum " % (
            table_name, data_time)

        print 'check_flag_chk_sql', check_flag_chk_sql

        check_flag_chk_result = mysql_conn_db.select(check_flag_chk_sql)

        print 'check_flag_chk_result', check_flag_chk_result

        # check_flag，Vt与hive同名表稽核结果标记
        check_flag_chk = 0
        if len(check_flag_chk_result) > 0: check_flag_chk = 1

        # vt
        count_vt_sql = "select count_num from tb_vt_check_result where table_name='%s' and partition='%s' " % (
            table_name, data_time)
        count_vt = mysql_conn_db.select(count_vt_sql)[0][0]

        sum_int_vt_sql = "select int_sum from tb_vt_check_result where table_name='%s' and partition='%s' " % (
            table_name, data_time)
        sum_int_vt = mysql_conn_db.select(sum_int_vt_sql)[0][0]

        sum_strint_vt_sql = "select end_string_sum from tb_vt_check_result where table_name='%s' and partition='%s' " % (
            table_name, data_time)
        sum_strint_vt = mysql_conn_db.select(sum_strint_vt_sql)[0][0]

        # hive
        count_hive_sql = "select count_num from tb_hive_check_result where table_name='%s' and partition='%s' " % (
            table_name, data_time)
        count_hive = mysql_conn_db.select(count_hive_sql)[0][0]

        sum_int_hive_sql = "select int_sum from tb_hive_check_result where table_name='%s' and partition='%s' " % (
            table_name, data_time)
        sum_int_hive = mysql_conn_db.select(sum_int_hive_sql)[0][0]

        sum_strint_hive_sql = "select end_string_sum from tb_hive_check_result where table_name='%s' and partition='%s' " % (
            table_name, data_time)
        sum_strint_hive = mysql_conn_db.select(sum_strint_hive_sql)[0][0]

        # hive_chk
        count_chk_sql = "select count_num from tb_hive_chk_check_result where table_name='%s' and partition='%s' " % (
            table_name, data_time)
        print 'count_chk_sql', count_chk_sql
        count_chk = mysql_conn_db.select(count_chk_sql)
        if len(count_chk) == 0: count_chk = ''
        print count_chk

        sum_int_chk_sql = "select int_sum from tb_hive_chk_check_result where table_name='%s' and partition='%s' " % (
            table_name, data_time)
        sum_int_chk = mysql_conn_db.select(sum_int_chk_sql)
        if len(sum_int_chk) == 0: sum_int_chk = ''

        sum_strint_chk_sql = "select end_string_sum from tb_hive_chk_check_result where table_name='%s' and partition='%s' " % (
            table_name, data_time)
        sum_strint_chk = mysql_conn_db.select(sum_strint_chk_sql)
        if len(sum_strint_chk) == 0: sum_strint_chk = ''

        # 更新mysql
        update_sql = "insert into tb_check_result (table_name,data_time,check_flag,check_flag_chk,count_vt,sum_int_vt,sum_strint_vt,count_hive,sum_int_hive,sum_strint_hive,count_chk,sum_int_chk,sum_strint_chk) values ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')  " % (
            table_name, data_time, check_flag, check_flag_chk, count_vt, sum_int_vt, sum_strint_vt, count_hive,
            sum_int_hive, sum_strint_hive, count_chk, sum_int_chk, sum_strint_chk)
        mysql_conn_db.insert(update_sql)



test = Output_result()
test.read_table_name()
