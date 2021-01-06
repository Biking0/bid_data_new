#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：ocdp_count.py
# 功能描述：过滤出分区表
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20201218
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：nohup python ocdp_count.py >> nohup.out_ocdp_20210106 &
# ***************************************************************************


import os
import ocdp_conn_db
import conn_local_db
import threading
import traceback
from Queue import Queue

table_file = open('./table_list.txt', 'r')
no_file = open('./no_list.txt', 'a+')
yes_file = open('./yes_count_list.txt', 'a+')
table_list = table_file.readlines()
table_file.close()


# 生成存储信息
def sy_count(table_name):
    du_h = "hadoop fs -du -h  /warehouse/tablespace/managed/hive/csap.db/"
    du_h_sh = du_h + table_name + ' > /home/ocdp/hyn/data_check/tmp_data_check/data_count/multi_data_count_du_h/table_info/' + table_name + '.txt'

    print 'du_h_sh:', du_h_sh

    os.popen(du_h_sh)

    # 解析读取存储信息
    get_size_info(table_name)

    # 获取存储信息

    # 解析读取存储


# 解析读取存储信息
def get_size_info(table_name):
    size_file = open(
        '/home/ocdp/hyn/data_check/tmp_data_check/data_count/multi_data_count_du_h/table_info/' + table_name + '.txt',
        'r')
    size_list = size_file.readlines()
    size_file.close()

    size_list_result = []

    # 遍历路径列表
    for i in range(len(size_list)):

        # 非正常路径
        if table_name not in size_list[i]:
            print 'error_path:', size_list[i]
            continue

        # print size_list[i]

        size, path = size_list[i].split(' ')[0], size_list[i].split(' ')[-1]

        # size_info_list=size_list[i].split(' ')
        #
        # # 去空
        # for j in size_info_list:
        #     if j == '':
        #         size_info_list.remove(j)

        # print 'size_info:',size, path

        size_list_result.append([size, path])

    get_part_list(table_name, size_list_result)
    # print 'size_info11:',size_list[i].split(' ')
    # print 'size_info11:',size_info_list


def get_part_list(table_name, size_list_result):
    # get_part_sql = "select part_name from tb_tmp_check_data where table_name='%s' " % (table_name)
    get_part_sql = "select part_name from tb_tmp_check_data where table_name='%s' and ocdp_count is null " % (
        table_name)

    print 'get_part_sql', get_part_sql

    part_list = conn_local_db.select(get_part_sql)

    # print 'part_list', part_list

    update_list = []

    for i in range(len(part_list)):

        # 处理多级分区
        if len(part_list[i][0].split('/')) > 1:

            part_name = part_list[i][0]

            multi_size_list_result = multi_part_name(table_name, part_name)

            # 分区不存在
            if len(multi_size_list_result) == 0:
                continue

            for m in range(len(multi_size_list_result)):
                print '多级分区存储信息：', multi_size_list_result[m]

                if part_name in multi_size_list_result[m][1]:
                    # print 'part_name_list:', part_name, size_list_result[j][1]
                    update_sql = "update tb_tmp_check_data set ocdp_count='%s' where table_name='%s' and part_name='%s'" % (
                        multi_size_list_result[m][0], table_name, part_name)

                    print 'update_sql', update_sql
                    update_list.append(update_sql)

                    # 找到目标分区，退出
                    break





        else:
            part_name = part_list[i][0].split('/')[0]
            # print 'part_name:', part_name

            for j in range(len(size_list_result)):
                if part_name in size_list_result[j][1]:
                    # print 'part_name_list:', part_name, size_list_result[j][1]
                    update_sql = "update tb_tmp_check_data set ocdp_count='%s' where table_name='%s' and part_name='%s'" % (
                        size_list_result[j][0], table_name, part_list[i][0])

                    print 'update_sql', update_sql
                    update_list.append(update_sql)
                    break

    conn_local_db.insert_batch(update_list)

    update_list = []
    size_list_result = []


# 处理多级分区
def multi_part_name(table_name, part_all_name):
    part_name_list = part_all_name.split('/')

    print 'part_all_name:', part_all_name

    multi_part_name = ''
    # 获取非最后一个分区，生成表名加分区
    for i in range(len(part_name_list) - 1):
        multi_part_name = multi_part_name + '/' + part_name_list[i]

    print 'multi_part_name', multi_part_name

    file_name = ' > /home/ocdp/hyn/data_check/tmp_data_check/data_count/multi_data_count_du_h_part/table_info/' + table_name + '_' + multi_part_name[
                                                                                                                                     1:].replace(
        '/', '') + '.txt'

    du_h = "hadoop fs -du -h  /warehouse/tablespace/managed/hive/csap.db/"
    du_h_sh = du_h + table_name + multi_part_name + file_name

    print 'du_h_sh:', du_h_sh

    try:
        os.popen(du_h_sh)

    except Exception as e:
        print '分区不存在', table_name, multi_part_name, e
        return []

    # 解析读取多分区存储信息
    size_list_result = get_multi_size_info(table_name, multi_part_name, file_name)

    return size_list_result


# 解析读取多分区存储信息，返回分区存储信息列表，[大小，路径]
def get_multi_size_info(table_name, multi_part_name, file_name):
    size_file = open(file_name[3:], 'r')
    size_list = size_file.readlines()
    size_file.close()

    size_list_result = []

    # 遍历路径列表
    for i in range(len(size_list)):

        # 非正常路径
        if table_name not in size_list[i]:
            print 'error_path:', size_list[i]
            continue

        # print size_list[i]

        size, path = size_list[i].split(' ')[0], size_list[i].split(' ')[-1]

        # size_info_list=size_list[i].split(' ')
        #
        # # 去空
        # for j in size_info_list:
        #     if j == '':
        #         size_info_list.remove(j)

        # print 'size_info:',size, path

        size_list_result.append([size, path])

    return size_list_result
    # print 'size_info11:',size_list[i].split(' ')
    # print 'size_info11:',size_info_list


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
            print traceback.print_exc()
            f = open('./error_info.log', 'a+')
            f.write(str(e) + '\n')

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
        line = line.strip('\n').replace('\t', '').replace(' ', '').replace('\r', '')

        print 1, ' #########################'
        print line
        multi_list.append(line)

        # 开始解析
        # create_desc(line)

        # 连续读取目标表
        # break

    multi_thread(multi_list)


read_table_name()
