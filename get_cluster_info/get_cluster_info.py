#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：get_cluster_info.py
# 功能描述：添加统计信息
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20201228
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python get_cluster_info.py
# ***************************************************************************

import os
import sys
import datetime as date_time

# 连接beeline
beeline = "beeline -u 'jdbc:hive2://172.19.40.241:10000/csap' -n csap -p 1q2w1q@W  --showHeader=false --outputformat=dsv --delimiterForDSV=$'\t' -e\""


# 用beeline获取维表任务
def get_task():
    get_task_sql = 'select * from tb_dim_cm_size_info'
    get_task_sh = beeline + get_task_sql + '\"'

    print 'get_task_sh', get_task_sh
    task_list = os.popen(get_task_sh).readlines()
    print 'task_list', task_list

    parse_task(task_list)


# 解析任务
def parse_task(task_list):
    result_list = []

    now_time = str(date_time.datetime.now())[0:10].replace('-', '').replace(' ', '').replace(':', '')

    for i in range(len(task_list)):
        task = task_list[i].split('\t')

        team = task[1]
        type = task[2]
        path = task[3]

        if type == 'hive':
            get_size_sh = "hadoop fs -du -s %s | awk -F ' ' '{print $2/1024/1024/1024}'" % (path)

            print 'get_size_sh', get_size_sh

            size = os.popen(get_size_sh).readline().replace('\n', '')
            print 'size', size

            result_list.append(
                "insert into table tb_hdfs_size_used partition(statis_date=%s) (id,team,type,size_used) values('%s','%s','%s','%s') ;" % (
                    now_time, i, team, type, size))


        else:
            get_size_sh = "hadoop fs -du -s %s | awk -F ' ' '{print $2/1024/1024/1024}'" % (path)

            print 'get_size_sh', get_size_sh

            size = os.popen(get_size_sh).readline().replace('\n', '')
            print 'size', size

            get_count_sh = "hadoop fs -count %s | awk -F ' ' '{print $2}'" % (path)

            print 'get_count_sh', get_count_sh

            count = os.popen(get_count_sh).readline().replace('\n', '')
            print 'count', count

            result_list.append(
                "insert into table tb_hdfs_size_used partition(statis_date=%s) (id,team,type,size_used,count_used) values('%s','%s','%s','%s','%s') ;" % (
                    now_time, i, team, type, size, count))

        # if i > 10:
        #     break

    print 'result_list', result_list

    put_data(result_list)


# 存储信息
def put_data(result_list):
    now_time = str(date_time.datetime.now())[0:10].replace('-', '').replace(' ', '').replace(':', '')

    # 清理表
    truncate_tb_sql = 'alter table tb_hdfs_size_used drop if exists partition (statis_date=%s)' % (now_time)

    truncate_tb_sh = beeline + truncate_tb_sql + '\"'
    print 'truncate_tb_sh', truncate_tb_sh

    os.popen(truncate_tb_sh)

    # 拼接sql
    for i in range(len(result_list)):
        insert_sh = beeline + result_list[i] + '\"'

        print 'insert_sh', insert_sh

        os.popen(insert_sh)

    print '插入数据完成'


get_task()
