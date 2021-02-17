#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：get_sql.py
# 功能描述：hive表数据稽核
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20200730
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python get_sql.py table_name
# ***************************************************************************

# 新集群执行


import os
import sys
import datetime

# 生产环境
# excute_desc_sh = "beeline -u 'jdbc:hive2://192.168.190.88:10000/csap' -n hive -p %Usbr7mx -e "
excute_desc_sh = "beeline -u 'jdbc:hive2://hua-dlzx2-a0202:10000/csap' -n ocdp -p 1q2w1q@W -e "


# 生成desc表结构文件
def create_desc(table_name):
    # 生产环境
    desc_sh = "beeline -u 'jdbc:hive2://hua-dlzx2-a0202:10000/csap' -n ocdp -p 1q2w1q@W -e 'desc  " + table_name + ' \' > ./' + table_name + '.txt'

    # print desc_sh
    os.popen(desc_sh).readlines()
    desc_parser(table_name)


# 解析desc表结构
def desc_parser(table_name):
    # desc_list = open('/home/ocdp/get_sql/' + table_name + '.txt', 'r').readlines()
    desc_list = open('./' + table_name + '.txt', 'r').readlines()

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

            # break

    end_string = ''
    # 列表逆序
    desc_list.reverse()
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
    desc_list = open('./' + line + '.txt', 'r').readlines()

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

        if 'Partition' not in line_list[1]:
            # print line_list[1], line_list[2], line_list[3],
            # print '#'
            pass

        # 检测分区数量
        if desc_list[i][2] == '#':
            check_partition_list = desc_list[i].split(' ')

            if check_partition_list[2] == 'Partition':
                # print '### 分区键'

                # partition_list = []
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
                    pass
                    # print '### 多个分区', line, partition_list
                    # check_result = open('/home/hive/hyn/hive_to_hive/check_result.txt', 'a+')
                    # check_result.write(line + ' ' + str(partition_list) + '\n')
                    # check_result.close()

                else:
                    pass
                    # print '### 1个分区', line, partition_list
                    # check_result = open('/home/hive/hyn/hive_to_hive/desc/check_result.txt', 'a+')
                    # check_result.write(line + ' ' + str(partition_list) + '\n')
                    # check_result.close()
            else:
                # print line, '无分区'
                pass

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
            chk_error = open('./chk_error.txt', 'a+')
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

    print '\n'
    print 'sql select :', sql

    # 执行查询
    select_sql_sh = excute_desc_sh + ' \" ' + sql
    # print '\n'
    # print select_sql_sh
    # os.popen(select_sql_sh).readlines()
    print '\n'
    # 删除表结构文本文件
    delete_sh = 'rm ./' + table_name + '.txt'
    os.popen(delete_sh).readlines()


# 启动
if __name__ == '__main__':
    input_length = len(sys.argv)
    print 'input_str: ', len(sys.argv)

    if input_length == 2:
        create_desc(sys.argv[1])

    else:
        print '输入表名，如:python get_sql.py table_name'
