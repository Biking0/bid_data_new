#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：hive_chk_data_check.py
# 功能描述：hive表数据稽核
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20210417
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python hive_data_check.py
# ***************************************************************************

# 1.从mysql库获取稽核表清单
# 2.分析hive库表结构，获取int字段，将所有表存到列表里
# 3.构造数据稽核sql，分析周期（分区），sum字段
# 4.抽取两个库的表文件到本地，进行对比

import os
import random
import time
import datetime
import config
# import pubUtil
import threading
from Queue import Queue
import mysql_conn_db


class Hive_data_check():

    def __init__(self):
        # chk_table_name = 'chk_result_' + table_num
        self.chk_table_name = 'tb_hive_chk_check_result'

        # 清理当天数据，支持重跑
        today = datetime.date.today()

        yestoday = today + datetime.timedelta(days=-1)

        first = today.replace(day=1)
        last_month = first - datetime.timedelta(days=1)

        # print '# yestoday', yestoday

        day_partition = 'statis_date=' + str(yestoday).replace('-', '')
        month_partition = 'statis_month=' + str(last_month).replace('-', '')[:6]

        delete_sql = " delete from %s where partition = '%s' or partition = '%s' " % (
            self.chk_table_name, day_partition, month_partition)

        print 'delete_sql', delete_sql
        mysql_conn_db.insert(delete_sql)

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
                    table_name = data_queque.get()+"_chk"

                    # print 'table_name', table_name
                    self.create_desc(table_name)

            except Exception as e:

                print e

                # 记录稽核异常日志
                f = open('./error_info.log', 'a+')
                f.write(str(e) + '\n')
                f.write(str(datetime.datetime.now()) + '\n')
                f.close()
                continue

    # 生成desc表结构文件
    def create_desc(self, table_name):
        # 生产环境
        desc_sh = config.excute_ocdp_sh + " desc  " + table_name + ' \' > ' + config.new_path + table_name + '.txt'

        print '# desc_sh', desc_sh

        os.popen(desc_sh).readlines()
        self.desc_parser(table_name)

    # 解析desc表结构
    def desc_parser(self, table_name):
        desc_list = open(config.new_path + table_name + '.txt', 'r').readlines()

        result_list = []

        for i in range(len(desc_list)):

            # 忽略其他行
            if desc_list[i][0] == '+':
                continue
            line_list = desc_list[i].strip().replace(' ', '').replace('\t', '').replace('\n', '').split('|')

            # 忽略表头
            if line_list[1] == 'col_name' or 'NULL' in line_list[1]:
                continue

            if 'Partition' not in line_list[1]:

                # 封装表结构int字段
                if line_list[2] == 'int':
                    result_list.append(line_list[1])

                # print '#'

            # 检测分区数量
            if desc_list[i][2] == '#':
                check_partition_list = desc_list[i].split(' ')

                if check_partition_list[2] == 'Partition':
                    # print '### 分区键'

                    for j in range(i + 1, len(desc_list)):

                        # 忽略其他行
                        if desc_list[j][0] == '+':
                            continue

                        if desc_list[j][3] == ' ':
                            continue
                        # print desc_list[j].split(' ')[1]

                    # 重要
                    break

                # print desc_list[i]
                continue
            #

            # break

        # todo，检测最后一个string类型字段
        end_string = ''
        # 列表逆序
        desc_list.reverse()
        print '##################'
        # print desc_list
        for i in range(len(desc_list)):

            # 忽略其他行
            if desc_list[i][0] == '+':
                continue
            line_list = desc_list[i].strip().replace(' ', '').replace('\t', '').replace('\n', '').split('|')

            # 忽略分区
            if line_list[1] == 'statis_date':
                continue

            # 忽略表头
            if line_list[1] == 'col_name' or 'NULL' in line_list[1]:
                continue

            if 'Partition' not in line_list[1]:
                print line_list[1], line_list[2], line_list[3]

                # print '########varchar', line_list[2][0:7]
                # 逆序后找到第一个string类型字段
                if line_list[2] == 'string' or line_list[2][0:7] == 'varchar':
                    end_string = line_list[1]

                    # 找到最后一个string，退出停止寻找
                    break
                    # result_list.append(line_list[1])

        # 封装表结构int字段
        # print 'int colume:'
        # print result_list

        # 分区检测
        self.check_partition(table_name, result_list, end_string)

    # 分区检测，构造分区，根据需要稽核的时间段，循环生成相应的分区，判断是否为分区表,line(table_name)
    def check_partition(self, line, result_list, end_string):
        desc_list = open(config.new_path + line + '.txt', 'r').readlines()

        # result_list = []
        partition_list = []

        for i in range(len(desc_list)):

            # 忽略其他行
            if desc_list[i][0] == '+':
                continue
            line_list = desc_list[i].strip().replace(' ', '').replace('\t', '').replace('\n', '').split('|')

            # 忽略表头
            if line_list[1] == 'col_name' or 'NULL' in line_list[1]:
                continue

            # 有分区
            if 'Partition' not in line_list[1]:
                pass
            # print line_list[1], line_list[2], line_list[3],

            # 检测分区数量
            if desc_list[i][2] == '#':
                check_partition_list = desc_list[i].split(' ')

                # 分区键
                if check_partition_list[2] == 'Partition':

                    for j in range(i + 1, len(desc_list)):

                        # 忽略其他行
                        if desc_list[j][0] == '+':
                            continue

                        if desc_list[j][3] == ' ':
                            continue
                        partition_key = desc_list[j].split(' ')[1]
                        # print partition_key
                        partition_list.append(partition_key)

                    if len(partition_list) > 1:
                        print '### 多个分区', line, partition_list
                        # check_result = open('/home/hive/hyn/hive_to_hive/check_result.txt', 'a+')
                        # check_result.write(line + ' ' + str(partition_list) + '\n')
                        # check_result.close()

                    # 单个分区
                    else:
                        pass
                        # print '### 1个分区', line, partition_list
                        # check_result = open('/home/hive/hyn/hive_to_hive/desc/check_result.txt', 'a+')
                        # check_result.write(line + ' ' + str(partition_list) + '\n')
                        # check_result.close()
                # 无分区
                else:
                    pass
                    # print line, '无分区'

                # 重要勿删，上一步分区已遍历完
                break

        # 分区处理
        print '# partition_list', partition_list

        partition = ''

        # 无分区表
        if len(partition_list) == 0:
            pass

        else:
            # 月分区，取上个月，前一个周期，以第一个分区为准
            if partition_list[0] == 'partition_month':
                today = datetime.date.today()
                first = today.replace(day=1)
                last_month = first - datetime.timedelta(days=1)
                last_month = last_month.strftime("%Y%m")
                # print '# last_month', last_month
                partition = 'partition_month=' + str(last_month).replace('-', '')

            # 日分区，取前一天，前一个周期
            elif partition_list[0] == 'statis_date':
                today = datetime.date.today()

                yestoday = today + datetime.timedelta(days=-1)

                # print '# yestoday', yestoday

                partition = 'statis_date=' + str(yestoday).replace('-', '')

            elif partition_list[0] == 'statis_month':
                today = datetime.date.today()
                first = today.replace(day=1)
                last_month = first - datetime.timedelta(days=1)
                last_month = last_month.strftime("%Y%m")
                # print '# last_month', last_month
                partition = 'statis_month=' + str(last_month).replace('-', '')

            # 其他分区，先不检测，记录到文件
            else:
                chk_error = open('./chk_error.txt', 'a+')
                chk_error.write(str(partition_list) + '\n')
                chk_error.write(str(datetime.datetime.now()) + '\n')
                chk_error.close()

        # 创建查询sql
        self.create_sql(line, result_list, partition, end_string)

    # 创建sql，进行查询,输入表名，int字段
    def create_sql(self, table_name, table_int_list, partition, end_string):
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
            table_int_str = table_int_str + "nvl(sum(%s),''),'_'," % (table_int_list[i])

        # print 'table_int_str', table_int_str

        sql_part2 = ",concat(%s)" % (table_int_str[0:-5])

        sql = sql_part1 + sql_part2 + sql_part3

        print 'sql select :', sql

        # 执行查询
        select_sql_sh = config.excute_ocdp_sh + ' \" ' + sql + ' \"'
        # print select_sql_sh

        self.insert_table(table_name, sql)

        # 删除表结构文本文件
        delete_sh = 'rm ' + table_name + '.txt'
        # os.popen(delete_sh).readlines()

    # 构造出sql，将查询结果插入mysql稽核结果表中
    def insert_table(self, table_name, sql):
        # 随机插入1-10稽核结果表
        table_num = str(random.randint(1, 10))

        # insert_sql = " use csap; insert into table " + chk_table_name + " partition (static_date=" + time.strftime(
        #     "%Y%m%d",
        #     time.localtime(
        #         time.time())) + ") " + sql
        # print insert_sql

        # 执行插入语句
        # insert_sql_sh = config.excute_ocdp_sh + ' \" ' + insert_sql + ' \" '

        # print insert_sql_sh
        # os.popen(insert_sql_sh).readlines()

        # 查询sql

        select_sql = sql
        select_sql_sh = config.result_ocdp_sh + ' \" ' + select_sql + ' \" '

        print "select_sql_sh", select_sql_sh

        select_result = os.popen(select_sql_sh).readlines()[0].replace('\n', '').split('\t')

        print select_result

        insert_sql = "insert into  " + self.chk_table_name + "  (data_source,table_name,partition,count_num,end_string_sum,int_sum,remark,update_time) values ('%s','%s','%s','%s','%s','%s','%s','%s')  " % (
        select_result[0], select_result[1], select_result[2], select_result[3], select_result[4], select_result[5],
        select_result[6], select_result[7])

        mysql_conn_db.insert(insert_sql)

        # export_chk_result(table_name)

    # 导出稽核结果表到文件excute_ocdp_sh
    def export_chk_result(self, table_name):
        export_sql = 'use csap; select DES_TBL,CYCLICAL,COUNT1,SUM1,REMARK from chk_result;'

        export_sh = config.excute_ocdp_sh + ' \" ' + export_sql + ' \" ' + ' >> chk_result.txt'

        print 'export_sh', export_sh

        os.popen(export_sh).readlines()

    # # 运行之前清理结果表分区，添加重跑功能
    # def clear_ocdp_partition(self):
    #     # 清理ocdp集群分区
    #     sql = "alter table chk_result drop if exists partition(static_date=" + pubUtil.get_today() + ");"
    #
    #     clear_sql_sh = config.excute_ocdp_sh + sql + '\''
    #
    #     print clear_sql_sh
    #
    #     # os.popen(clear_sql_sh)


# test = Hive_data_check()
# test.read_table_name()
