#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：copy_data_sy_to_ocdp_day.py
# 功能描述：迁移Hive表
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20200805
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python copy_data_sy_to_ocdp_day.py
# ***************************************************************************

import os
import sys
import random
import random
import string
from datetime import datetime
import datetime as date_time
import conn_db
import pymysql

# 对分区表、非分区表分类

# 功能结构
# 读取表名
# 处理日期输入数据
# 构造迁移语句
# 执行迁移语句
# 分区表直接拷贝文件夹
# 打印日志


day_table = 40
partition_statis_date = 'statis_date'

# 迁移批次，0：日表，1：月表，2：年表，4：维表，全量
migration_batch = 0
int_0 = 0

# 任务存储文件
get_task_file = './get_task_file.txt'

# 日表开始迁移日期
start_date = '20200801'
end_date = '20200805'

# 日期格式
day_format = '%Y%m%d'

# 连接mysql
mysql_sh = "mysql -h 172.19.168.22 -P 3308 -u zhao -pzhao zhao -e ' "

file_path = '/home/ocdp/hyn/copy_hdfs_data/'

# 同步状态

# 未同步
copy_status_0 = '0'
# 正在同步
copy_status_1 = '1'

# 稽核状态

chk_status_0 = ''

data_source = 'sy'


# 获取任务，mysql获取表名，每次获取一个列表进行遍历
def read_table_name():
    # 获取可以稽核表名列表
    get_task_sql = "select a.table_name from tb_copy_get_task a left join tb_copy_data_log b on a.table_name=b.table_name where b.copy_status='0' or b.table_name is null ;"

    print get_task_sql

    # get_task_sql_sh = mysql_sh + get_task_sql + '\' > ' + get_task_file

    select_result = conn_db.select(get_task_sql)
    print select_result
    print type(select_result)

    # 遍历查询结果
    for i in select_result:
        table_name = i[0]
        print '表名：', table_name

        # 调用迁移
        input_date(table_name)

    # 执行获取sql
    # os.popen(get_task_sql_sh)

    # get_task_list = open(get_task_file, 'r')

    multi_list = []

    # for table_name in get_task_list.readlines():
    #     table_name = table_name.strip('\n').replace('\t', '').replace(' ', '')
    #
    #     print 1, ' #########################'
    #     print table_name
    #
    #     multi_list.append(table_name)
    #
    #     # 调用迁移
    #     input_date(table_name)


# 处理日期输入数据，数据迁移日期
def input_date(table_name):
    start_date_time = datetime.strptime(start_date, day_format)

    end_date_time = datetime.strptime(end_date, day_format)

    # 迁移周期跨度
    date_length = (end_date_time - start_date_time).days

    print '迁移周期：', date_length

    # print type((end_date_time-start_date_time).days)

    partition_date_init = start_date_time

    # 遍历迁移周期
    for i in range(date_length):
        partition_date = str((partition_date_init + date_time.timedelta(days=i + 1)).date()).replace('-', '')
        print partition_date

        # 检测该周期是否已迁移完成
        if check_date(table_name, partition_date):
            # 返回结果不为空
            continue

        # 更新mysql同步状态
        update_copy_status = "update tb_copy_data_log set copy_status = '1' , where  "

        # 添加分区
        add_partition(table_name, partition_date)

        # 单条测试，正式上线后删掉
        break


# 检测该周期是否已迁移完成
def check_date(table_name, partition_date):
    # 检测该表是否存在
    check_table_sql = "select table_name from tb_copy_data_log where table_name='" + table_name + "\'"

    print '检测该表是否存在:', check_table_sql

    check_table_result = conn_db.select(check_table_sql)

    # 当前时间
    now_time = str(date_time.datetime.now())[0:19]

    if not check_table_result:
        # 初始化mysql同步状态
        insert_table_sql = "insert into tb_copy_data_log (data_source,table_name,partition_type,partition_time,copy_status,chk_status,start_time,end_time) values('%s','%s','%s','%s','%s','%s','%s','%s')" % (
            data_source, table_name, partition_statis_date, partition_date, copy_status_1, chk_status_0, now_time, '0')
        print '插入记录初始化：', insert_table_sql

        conn_db.insert(insert_table_sql)

        # 测试
        conn_db.insert("insert into test (id) values ('123')")
        print '完成初始化'

    # 检测该分区是否存在
    check_partition_sql = "select table_name from tb_copy_data_log where table_name='" + table_name + "' and partition_time='%s\'" % (
        partition_date)
    print check_partition_sql
    check_partition_result = conn_db.select(check_partition_sql)
    if not check_partition_result:
        # 插入分区
        insert_table_sql = "insert into tb_copy_data_log (data_source,table_name,partition_type,partition_time,copy_status,chk_status,start_time,end_time) values('%s','%s','%s','%s','%s','%s','%s','%s')" % (
            data_source, table_name, partition_statis_date, partition_date, copy_status_1, chk_status_0, now_time, '0')
        print '插入记录初始化：', insert_table_sql
        check_table_result = conn_db.insert(insert_table_sql)

    # 检测同步状态
    check_status_sql = " select table_name from tb_copy_data_log where table_name='" + table_name + "' and partition_time= '" + partition_date + "' and copy_status = '" + copy_status_0 + "\'"

    print check_status_sql
    select_result = conn_db.select(check_status_sql)

    print '检测结果：', select_result

    # 结果判空
    if select_result:

        return True
    else:
        return False


# 添加分区
def add_partition(table_name, partition_date):
    # 生成周期
    # for i in ()

    # 先测试单周期
    add_partition_sql = "alter table " + table_name + " add partition (" + partition_statis_date + "=" + partition_date + ")"

    print add_partition_sql


# 构造迁移语句
def create_sql(table_name, date):
    # hadoop distcp -i hdfs://192.168.190.89:8020/apps/hive/warehouse/csap.db/tb_si_cu_voma_limit_whitelist_day/statis_date=20170617 hdfs://172.19.168.4:8020/warehouse/tablespace/managed/hive/tb_si_cu_voma_limit_whitelist_day

    distcp_sh = "hadoop distcp -i hdfs://192.168.190.89:8020/apps/hive/warehouse/csap.db/" + table_name + "/" + partition_statis_date + "=" + date + " hdfs://172.19.168.4:8020/warehouse/tablespace/managed/hive/" + table_name + "/"

    print distcp_sh


# 执行迁移语句
def exec_sql():
    pass


read_table_name()