# !/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：check_error.py
# 功能描述：检查迁移报错
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20201018
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：nohup python check_error.py >> nohup.out &
# ***************************************************************************

# 检查迁移报错，若分区不存在，将日志表拷贝状态改为2，remark2填分区不存在

import os
import sys
import conn_db
import time

sy_hdfs_path = "hadoop fs -ls hdfs://192.168.190.88:8020/apps/hive/warehouse/csap.db/"

update_sql_list = []
start_time = 0


def get_error_log():
    global start_time
    try:
        get_error_sql = "select id,table_name,partition_type,partition_time,copy_status from tb_copy_data_log where  remark2 is null and remark5 ='' and partition_type<>'all' and ( copy_status='3' or sy_count<>ocdp_count or sy_count is null or ocdp_count is null) limit 200"

        print 'get_error_sql', get_error_sql
        get_error_result = conn_db.select(get_error_sql)

        start_time = time.time()

        # # 遍历集合，更新此批次状态,status=1
        update_status_list = []
        update_sql = "update tb_copy_data_log  set remark5='checked' "
        in_condition = ''
        for i in get_error_result:
            in_condition = str(i[0]) + "," + in_condition

        update_sql = update_sql + "where id in (" + in_condition + "0)"

        # print '更新此批次状态', update_sql
        result = conn_db.insert(update_sql)


        for i in get_error_result:
            try:
                table_name, partition_type, partition_time, copy_status = i[1], i[2], i[3], i[4]
                check_partition(table_name, partition_type, partition_time, copy_status)
            except Exception as e:
                print e
                print '异常2'
        # 批量更新mysql
        update_mysql_batch()
    except Exception as e:
        print e
        print '异常1'


def check_partition(table_name, partition_type, partition_time, copy_status):
    print 'get result:', table_name, partition_type, partition_time, copy_status

    check_sh = sy_hdfs_path + table_name + "/" + partition_type + "=" + partition_time

    check_result = os.system(check_sh)

    # 分区不存在
    if check_result != 0:
        update_mysql(table_name, partition_time)

    print 'check_sh:', check_sh


def update_mysql(table_name, partition_time):
    update_sql = "update tb_copy_data_log set copy_status ='2' ,remark2='partition not exist' where table_name='" + table_name + "' and partition_time='" + partition_time + "\'"

    update_sql_list.append(update_sql)
    # conn_db.insert(update_sql)

    print 'update_sql', update_sql


# 批量更新mysql
def update_mysql_batch():
    global update_sql_list,start_time
    conn_db.insert_batch(update_sql_list)
    print update_sql_list

    print '更新数据量：', len(update_sql_list),'耗时',time.time()-start_time
    update_sql_list = []
    start_time=0



