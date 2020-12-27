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
from datetime import datetime
import datetime as date_time
import conn_db

# 对分区表、非分区表分类
# 功能结构
# 读取表名
# 处理日期输入数据
# 构造迁移语句
# 执行迁移语句
# 分区表直接拷贝文件夹
# 打印日志


partition_statis_date = 'statis_date'

# 迁移批次，0：日表，1：月表，2：年表，4：维表，全量
migration_batch = 0
int_0 = 0

# 日期格式
day_format = '%Y%m%d'

# 连接mysql
mysql_sh = "mysql -h 172.19.168.22 -P 3308 -u zhao -pzhao zhao -e ' "
excute_hive_sh = "beeline -u 'jdbc:hive2://hua-dlzx2-a0202:10000/csap' -n ocdp -p 1q2w1q@W -e '"

# 同步状态

# 未同步
copy_status_0 = '0'
# 正在同步
copy_status_1 = '1'
# 同步完成
copy_status_2 = '2'

# 稽核状态

chk_status_0 = ''

data_source = 'sy'


# 获取任务，mysql获取表名，每次获取一个列表进行遍历
def read_table_name():
    # 获取可以稽核表名列表
    get_task_sql = "select table_name  from tb_copy_get_task where start_partition is not null and end_partition is not null and ifnull(now_partition ,start_partition) < end_partition;"

    print '获取任务sql：', get_task_sql

    select_result = conn_db.select(get_task_sql)
    print '获取任务：', select_result

    # 遍历任务列表
    for i in select_result:
        table_name = i[0]
        print '表名：', table_name

        # 调用迁移
        input_date(table_name)

    print '无迁移任务'


# 处理日期输入数据，数据迁移日期，从数据库获取开始日期和结束日期
def input_date(table_name):
    get_date_sql = "select ifnull(now_partition,start_partition) as start_partition,end_partition from tb_copy_get_task where table_name='" + table_name + "\'"

    print '获取开始日期:', get_date_sql
    select_result = conn_db.select(get_date_sql)

    start_date = select_result[0][0]
    end_date = select_result[0][1]

    start_date_time = datetime.strptime(start_date, day_format)

    end_date_time = datetime.strptime(end_date, day_format)

    # 迁移周期跨度
    date_length = (end_date_time - start_date_time).days + 1

    print '迁移周期：', date_length

    partition_date_init = start_date_time

    # 遍历迁移周期
    for i in range(date_length):
        print i
        partition_date = str((partition_date_init + date_time.timedelta(days=i)).date()).replace('-', '')
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
        # break


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

    # 重建分区
    delete_partition_sql = "alter table " + table_name + " drop if  exists partition (" + partition_statis_date + "=" + partition_date + ");"
    delete_partition_sql_sh = excute_hive_sh + delete_partition_sql + "\'"

    # 先测试单周期
    add_partition_sql = "alter table " + table_name + " add if not exists partition (" + partition_statis_date + "=" + partition_date + ");"

    print add_partition_sql

    add_partition_sql_sh = excute_hive_sh + add_partition_sql + "\'"

    print '分区已添加：', delete_partition_sql_sh, add_partition_sql_sh
    os.popen(delete_partition_sql_sh)
    os.popen(add_partition_sql_sh)

    copy_data(table_name, partition_date)


# 构造迁移语句
def copy_data(table_name, partition_date):
    # hadoop distcp -i hdfs://192.168.190.89:8020/apps/hive/warehouse/csap.db/tb_si_cu_voma_limit_whitelist_day/statis_date=20170617 hdfs://172.19.168.4:8020/warehouse/tablespace/managed/hive/tb_si_cu_voma_limit_whitelist_day
    st_time = date_time.datetime.now()
    print "[info]" + str(st_time), ":表数据迁移开始:", table_name, "分区:", partition_date
    distcp_sh = "hadoop distcp -i hdfs://192.168.190.89:8020/apps/hive/warehouse/csap.db/" + table_name + "/" + partition_statis_date + "=" + partition_date + " hdfs://172.19.168.4:8020/warehouse/tablespace/managed/hive/csap.db/" + table_name + "/"

    print distcp_sh

    os.popen(distcp_sh)
    end_time = date_time.datetime.now()
    print "[info]" + str(end_time), ":表数据迁移结束:", table_name, "分区:", partition_date
    print '共耗时:', end_time - st_time, 'S'
    add_info(table_name, partition_date)
    copy_ok(table_name, partition_date, st_time, end_time)


# 数据迁移完成更新数据库记录
def copy_ok(table_name, partition_date, st_time, end_time):
    update_status_sql = "update tb_copy_data_log set copy_status ='" + copy_status_2 + "',start_time='" + str(st_time)[
                                                                                                          0:19] + "',end_time='" + str(
        end_time)[0:19] + "' where table_name='" + table_name + "' and partition_time='" + partition_date + "\'"

    print '更新sql:', update_status_sql

    conn_db.insert(update_status_sql)

    # 更新now_date
    update_task_sql = "update tb_copy_get_task set now_partition ='" + partition_date + "' where table_name='" + table_name + "\'"

    print '更新sql:', update_task_sql

    conn_db.insert(update_task_sql)

    print '数据迁移同步,更新同步状态完成:', table_name, partition_date
    if str(date_time.datetime.now())[11:13] == '06':
        print '到达早上6点,自动停止迁移任务,当前时间:', str(date_time.datetime.now())[0:19]
        exit(0)


def add_info(table_name, partition_date):
    add_info_sql = "ANALYZE TABLE " + table_name + " partition(" + partition_statis_date + "= " + partition_date + " ) COMPUTE STATISTICS;"

    add_info_sql_sh = excute_hive_sh + add_info_sql + "\'"

    print '收集元数据库统计信息:', add_info_sql_sh

    os.popen(add_info_sql_sh)

    print '[info] ', str(date_time.datetime.now())[0:19], ':收集元数据库统计信息完成：', table_name, '分区：', partition_date


read_table_name()
