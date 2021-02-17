#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：sy_hive_data_check.py
# 功能描述：hive表数据稽核
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20200624
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python sy_hive_data_check.py
# ***************************************************************************

# 1. 分析hive库表结构，获取int字段，将所有表存到列表里
# 2. 构造数据稽核sql，分析周期（分区），sum字段
# 3. 抽取两个库的表文件到本地，进行对比

import os
import sys
import time
import datetime
import config
import pubUtil
import threading
from Queue import Queue

# 生产环境
# excute_desc_sh = "hive -e "

# 测试环境
# excute_desc_sh = "hive -e "
excute_desc_sh = "beeline -u 'jdbc:hive2://192.168.190.88:10000/csap' -n hive -p %Usbr7mx -e "


# 生成desc表结构文件
def create_desc(table_name):
    # 生产环境
    desc_sh = "beeline -u 'jdbc:hive2://192.168.190.88:10000/csap' -n hive -p %Usbr7mx -e 'desc  " + table_name + ' \' > /home/hive/hyn/data_check/' + table_name + '.txt'

    # 测试环境
    # desc_sh = "beeline -u 'jdbc:hive2://172.22.248.19:10000/default' -n csap -p @WSX2wsx -e 'desc  " + table_name + ' \' > ./' + table_name + '.txt'

    # print desc_sh
    os.popen(desc_sh).readlines()
    desc_parser(table_name)


# 解析desc表结构
def desc_parser(table_name):
    desc_list = open('/home/hive/hyn/data_check/' + table_name + '.txt', 'r').readlines()

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
            # print line_list[1], line_list[2], line_list[3],

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
    # print '##################'
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
            # print line_list[1], line_list[2], line_list[3]

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
    desc_list = open('/home/hive/hyn/data_check/' + line + '.txt', 'r').readlines()

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

        # if 'Partition' not in line_list[1]:
        #     # print line_list[1], line_list[2], line_list[3],
        #     print '#'

        # 检测分区数量
        if desc_list[i][2] == '#':
            check_partition_list = desc_list[i].split(' ')

            if check_partition_list[2] == 'Partition':
                print '### 分区键'

                # partition_list = []
                for j in range(i + 1, len(desc_list)):

                    # 忽略其他行
                    if desc_list[j][0] == '+':
                        continue

                    if desc_list[j][3] == ' ':
                        continue
                    partition_key = desc_list[j].split(' ')[1]
                    print partition_key
                    partition_list.append(partition_key)

                if len(partition_list) > 1:
                    print '### 多个分区', line, partition_list
                    # check_result = open('/home/hive/hyn/hive_to_hive/check_result.txt', 'a+')
                    # check_result.write(line + ' ' + str(partition_list) + '\n')
                    # check_result.close()

                else:
                    print '### 1个分区', line, partition_list
                    # check_result = open('/home/hive/hyn/hive_to_hive/desc/check_result.txt', 'a+')
                    # check_result.write(line + ' ' + str(partition_list) + '\n')
                    # check_result.close()
            else:
                print line, '无分区'

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
            print '# last_month', last_month
            partition = 'partition_month=' + str(last_month).replace('-', '')

        # 日分区，取前一天，前一个周期
        elif partition_list[0] == 'statis_date':
            today = datetime.date.today()

            yestoday = today + datetime.timedelta(days=-1)

            print '# yestoday', yestoday

            partition = 'statis_date=' + str(yestoday).replace('-', '')

        elif partition_list[0] == 'statis_month':
            today = datetime.date.today()
            first = today.replace(day=1)
            last_month = first - datetime.timedelta(days=1)
            last_month = last_month.strftime("%Y%m")
            print '# last_month', last_month
            partition = 'statis_month=' + str(last_month).replace('-', '')

        # 其他分区，先不检测，记录到文件
        else:
            chk_error = open('/home/hive/hyn/data_check/chk_error.txt', 'a+')
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
    print select_sql_sh
    # os.popen(select_sql_sh).readlines()

    insert_table(table_name, sql, select_sql_sh)

    # 删除表结构文本文件
    delete_sh = 'rm ' + table_name + '.txt'
    # os.popen(delete_sh).readlines()


# 构造出sql，将查询结果插入稽核结果表中
def insert_table(table_name, sql, select_sql_sh):
    # chk_table_name = 'chk_result'
    chk_table_name = 'chk_result'
    insert_sql = " use csap;insert into table " + chk_table_name + " partition (static_date=" + time.strftime(
        "%Y%m%d",
        time.localtime(
            time.time())) + ") " + sql
    print insert_sql

    # 执行插入语句
    insert_sql_sh = excute_desc_sh + ' \" ' + insert_sql + ' \" '
    print insert_sql_sh
    # os.popen(insert_sql_sh).readlines()

    # 导出数据到文件
    # export_chk_result(table_name)

    # 苏研数据迁移到ocdp集群
    # distcp_sy_to_ocdp()
    insert_mysql(table_name, insert_sql, select_sql_sh)


# 将数据插入mysql表处理并发问题
def insert_mysql(table_name, sql, select_sql_sh):
    # select_sql_sh = excute_desc_sh + ' ' + '\"use csap; ' + sql + ';\"'
    print '#select_sql_sh', select_sql_sh

    select_result = os.popen(select_sql_sh).readlines()
    print 'select_result', select_result

    result_str = str(select_result[3].replace(' ', '').replace('\t', '').replace('\n', ''))
    result_list = result_str.split('|')

    print 'result_str', result_str
    print 'result_list', result_list
    # f = open("./"++".txt", 'w')
    # f.write(str(select_result[3].replace(' ', '').replace('\t', '').replace('\n', '')))

    # insert_sql = "mysql -ucsapdmcfg -h192.168.195.233 -P20031 -s -r -p -A -N -piEXIMt3w\!TFL9vkO csapdmcfg -e \"insert into test_thread (id,name) values('1','123');\""
    insert_sql = "mysql -ucsapdmcfg -h192.168.195.233 -P20031 -s -r -p -A -N -piEXIMt3w\!TFL9vkO csapdmcfg -e \"insert into tb_chk_result_test2(data_source ,des_tbl ,cyclical,count1,end_string_sum,sum1,remark,chk_dt,static_date) values('%s','%s','%s','%s','%s','%s','%s','%s','');\"" % (
        result_list[1], result_list[2], result_list[3], result_list[4], result_list[5], result_list[6], result_list[7],
        result_list[8])

    print 'insert_sql', insert_sql
    os.popen(insert_sql)


# 导出稽核结果表到文件
def export_chk_result(table_name):
    export_sql = "use csap; select DES_TBL,CYCLICAL,COUNT1,SUM1,REMARK from %s;" % (table_name)

    export_sh = excute_desc_sh + ' \" ' + export_sql + ' \" ' + ' >> %s.txt' % (table_name)

    print 'export_sh', export_sh

    os.popen(export_sh).readlines()


# 将苏研集群稽核表数据迁移到oadp集群，先迁移数据后添加分区
def distcp_sy_to_ocdp():
    partition = 'static_date=' + pubUtil.get_today()

    # 迁移之前先清理
    # clear_sh = "hadoop fs -rm -r hdfs://172.19.168.4:8020/warehouse/tablespace/managed/hive/sy_chk_result/" + partition
    sql = "alter table sy_chk_result drop if exists partition(static_date=" + pubUtil.get_today() + ");"
    clear_sh = config.excute_ocdp_sh + '\' ' + sql + '\''
    # 迁移数据不能用，使用mysql通信
    distcp_sh = 'hadoop distcp -i hdfs://192.168.190.89:8020/apps/hive/warehouse/csap.db/chk_result/' + partition + ' hdfs://172.19.168.4:8020/warehouse/tablespace/managed/hive/sy_chk_result/'

    # 添加分区
    add_partition_sql = "alter table sy_chk_result add if not exists partition(static_date=" + pubUtil.get_today() + ");"
    add_partition_sh = config.excute_ocdp_sh + '\' ' + add_partition_sql + '\''

    print 'clear_sh', clear_sh
    print 'distcp_sh', distcp_sh
    print 'add_partition_sh', add_partition_sh

    # 执行迁移
    os.popen(clear_sh)
    # os.popen(distcp_sh)
    # os.popen(add_partition_sh)


# 读取表名
def read_table_name():
    f = open('/home/hive/hyn/data_check/test_table_name.txt', 'r')

    multi_list = []

    for line in f.readlines():
        line = line.strip('\n').replace('\t', '')
        #
        # print 1, ' #######################'
        # print line
        multi_list.append(line)

        # 开始解析
        # create_desc(line)

        # 连续读取目标表
        # break

    multi_thread(multi_list)

    # 清理tb*文件
    # pubUtil.clear_tb_file()


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
    a = 1
    # list分块，调用多线程
    for i in range(a):
        # list分块，调用多线程
        multi1 = threading.Thread(target=read_list, args=(5, data_queque, result_queque))

        multi1.start()


# 运行之前清理结果表分区，添加重跑功能
def clear_sy_partition():
    # 清理苏研集群chk_result分区，insert overwrite，使用数据覆盖
    sql = 'insert overwrite table chk_result partition(static_date=' + pubUtil.get_today() + ') select data_source,des_tbl,cyclical,count1,sum1,remark,chk_dt from chk_result where static_date=20091010'

    clear_sql_sh = config.excute_sy_sh + sql + '\''

    # print clear_sql_sh

    # 执行清理，多线程运行不能清理分区
    # os.popen(clear_sql_sh)

    read_table_name()


clear_sy_partition()
