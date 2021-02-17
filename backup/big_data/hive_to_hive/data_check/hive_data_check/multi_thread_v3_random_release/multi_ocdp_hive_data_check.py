#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：multi_ocdp_hive_data_check.py
# 功能描述：hive表数据稽核
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20200624
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python multi_ocdp_hive_data_check.py
# ***************************************************************************

# 1. 分析hive库表结构，获取int字段，将所有表存到列表里
# 2. 构造数据稽核sql，分析周期（分区），sum字段
# 3. 抽取两个库的表文件到本地，进行对比

import os
import random
import time
import datetime
import config
import pubUtil
import threading
from Queue import Queue

# 生产环境
# excute_desc_sh = "beeline -u 'jdbc:hive2://192.168.190.88:10000/csap' -n hive -p %Usbr7mx -e "
excute_desc_sh = "beeline -u 'jdbc:hive2://172.19.40.241:10000/csap' -n ocdp -p 1q2w1q@W -e "


# 测试环境
# excute_desc_sh = "beeline -u 'jdbc:hive2://172.22.248.19:10000/default' -n csap -p @WSX2wsx -e "


# 生成desc表结构文件
def create_desc(table_name):
    # 生产环境
    desc_sh = "beeline -u 'jdbc:hive2://172.19.40.241:10000/csap' -n ocdp -p 1q2w1q@W -e 'desc  " + table_name + ' \' > /home/ocdp/hyn/data_check/hive_data_check/' + table_name + '.txt'

    # 测试环境
    # desc_sh = "beeline -u 'jdbc:hive2://172.22.248.19:10000/default' -n csap -p @WSX2wsx -e 'desc  " + table_name + ' \' > ./' + table_name + '.txt'

    os.popen(desc_sh).readlines()
    desc_parser(table_name)


# 解析desc表结构
def desc_parser(table_name):
    desc_list = open('/home/ocdp/hyn/data_check/hive_data_check/' + table_name + '.txt', 'r').readlines()

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
    check_partition(table_name, result_list, end_string)


# 分区检测，构造分区，根据需要稽核的时间段，循环生成相应的分区，判断是否为分区表,line(table_name)
def check_partition(line, result_list, end_string):
    desc_list = open('/home/ocdp/hyn/data_check/hive_data_check/' + line + '.txt', 'r').readlines()

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
    # print '# partition_list', partition_list

    partition = ''

    # 无分区表
    if len(partition_list) == 0:
        pass

    else:
        # 月分区，取上个月，前一个周期
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
            chk_error = open('/home/ocdp/hyn/data_check/hive_data_check/chk_error.txt', 'a+')
            chk_error.write(str(partition_list))
            chk_error.close()

    # 创建查询sql
    create_sql(line, result_list, partition, end_string)


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
        table_int_str = table_int_str + "nvl(sum(%s),''),'_'," % (table_int_list[i])

    # print 'table_int_str', table_int_str

    sql_part2 = ",concat(%s)" % (table_int_str[0:-5])

    sql = sql_part1 + sql_part2 + sql_part3

    print 'sql select :', sql

    # 执行查询
    select_sql_sh = excute_desc_sh + ' \" ' + sql + ' \"'
    # print select_sql_sh

    insert_table(table_name, sql)

    # 删除表结构文本文件
    delete_sh = 'rm ' + table_name + '.txt'
    # os.popen(delete_sh).readlines()


# 构造出sql，将查询结果插入稽核结果表中
def insert_table(table_name, sql):
    # 随机插入1-10稽核结果表
    table_num = str(random.randint(1, 10))

    chk_table_name = 'chk_result_' + table_num
    insert_sql = " use csap; insert into table " + chk_table_name + " partition (static_date=" + time.strftime("%Y%m%d",
                                                                                                               time.localtime(
                                                                                                                   time.time())) + ") " + sql
    # print insert_sql

    # 执行插入语句
    insert_sql_sh = excute_desc_sh + ' \" ' + insert_sql + ' \" '
    print insert_sql_sh
    os.popen(insert_sql_sh).readlines()

    # export_chk_result(table_name)


# 获取表结构
def get_table_struct(table_name):
    pass


# 导出稽核结果表到文件excute_ocdp_sh
def export_chk_result(table_name):
    export_sql = 'use csap; select DES_TBL,CYCLICAL,COUNT1,SUM1,REMARK from chk_result;'

    export_sh = excute_desc_sh + ' \" ' + export_sql + ' \" ' + ' >> chk_result.txt'

    print 'export_sh', export_sh

    os.popen(export_sh).readlines()


# 将苏研集群稽核表迁移到oadp集群
def distcp_sy_to_ocdp():
    # ocdp集群添加分区

    add_partition_sh = "beeline -u 'jdbc:hive2://172.19.40.241:10000/csap' -n ocdp -p 1q2w1q@W -e " + 'alter table '


# 对比数据，废弃该方法
def diff_data():
    pass


# 读取表名
def read_table_name():
    f = open('/home/ocdp/hyn/data_check/hive_data_check/test_table_name.txt', 'r')
    i = 1

    multi_list = []

    for line in f.readlines():
        line = line.strip('\n').replace(' ', '').replace('\t', '')

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
                create_desc(table_name)

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
    a = 150
    # list分块，调用多线程
    for i in range(a):
        # list分块，调用多线程
        multi1 = threading.Thread(target=read_list, args=(5, data_queque, result_queque))

        multi1.start()


# 运行之前清理结果表分区，添加重跑功能
def clear_ocdp_partition():
    # 清理ocdp集群分区
    sql = "alter table chk_result drop if exists partition(static_date=" + pubUtil.get_today() + ");"

    clear_sql_sh = config.excute_ocdp_sh + sql + '\''

    print clear_sql_sh

    # os.popen(clear_sql_sh)


read_table_name()
