#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：copy_data_sy_to_ocdp.py
# 功能描述：迁移Hive表
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20200805
# 修改日志：20200810
# 修改日期：
# ***************************************************************************
# 程序调用格式：# python copy_data_sy_to_ocdp_all_table.py 1 1 0 60 60
# 程序调用格式：# python copy_data_sy_to_ocdp_all_table.py 1 1 0 20 20
# ***************************************************************************

import os
import sys
from datetime import datetime
import datetime as date_time
import conn_db
import config


# 对分区表、非分区表分类
# 功能结构
# 读取表名
# 处理日期输入数据
# 构造迁移语句
# 执行迁移语句
# 分区表直接拷贝文件夹
# 打印日志

class CopyData():

    # 参数初始化
    def __init__(self, table_type, input_batch, size_type, bandwidth, map_num, all=None):
        self.table_type = table_type
        self.input_batch = input_batch
        self.size_type = size_type
        self.bandwidth = bandwidth
        self.map_num = map_num
        self.all = all

    # 获取任务，mysql获取表名，每次获取一个列表进行遍历
    def read_table_name(self):

        # 获取可以稽核表名列表
        get_task_sql = "select a.table_name ,a.partition_type from tb_copy_get_task a left join tb_copy_data_log b on a.table_name=b.table_name where copy_status='" + config.copy_status_2 + "' and table_type='" + self.table_type + "' and size_type='" + self.size_type + "' and  migration_batch= '" + input_batch + "';"

        print '获取任务sql：', get_task_sql

        select_result = conn_db.select(get_task_sql)
        print '获取任务：', select_result

        # 全量表
        if self.table_type == '1':
            # 遍历任务表名列表
            for i in select_result:
                table_name = i[0]
                print '表名：', table_name

                # 调用迁移
                self.input_date(table_name, partition_date=None)

        # 日分区表
        elif self.table_type == '2':

            # 分区键
            partition_type = select_result[0][1]

            print '获取分区键', partition_type

            # 日表迁移33天
            day_partition_list = config.day_partition.reverse()
            for partition_date in day_partition_list:

                # 遍历任务列表
                for i in select_result:
                    table_name = i[0]
                    print '表名：', table_name

                    # 调用迁移
                    self.input_date(table_name, partition_date)

        print '无迁移任务'

        # 日分区

    # 处理日期输入数据，数据迁移日期，从数据库获取开始日期和结束日期
    def input_date(self, table_name, partition_date):

        # 全量表
        if self.table_type == '1':
            if self.check_date(table_name, partition_date=None, error=None):

                try:
                    self.add_partition(table_name, partition_date=None)
                except Exception as e:

                    print '全量表-出现异常：', table_name, e

                    self.check_date(table_name, partition_date=None, error=True)

                # 更新mysql失败

            # 该表已迁移
            else:
                print '该表已迁移', table_name
                return

        # 非全量表，todo
        else:

            # 获取分区键，分区时间
            get_date_sql = "select ifnull(now_partition,start_partition) as start_partition,end_partition from tb_copy_get_task where table_name='" + table_name + "\'"

            print '获取开始日期:', get_date_sql
            select_result = conn_db.select(get_date_sql)

            start_date = select_result[0][0]
            end_date = select_result[0][1]

            start_date_time = datetime.strptime(start_date, config.day_format)

            end_date_time = datetime.strptime(end_date, config.day_format)

            # 迁移周期跨度
            date_length = (end_date_time - start_date_time).days + 1

            print '迁移周期：', date_length

            partition_date_init = start_date_time

            # 遍历迁移周期
            for i in range(date_length):

                print i
                partition_date = str((partition_date_init + date_time.timedelta(days=i)).date()).replace('-', '')

                print partition_date

                try:

                    # 检测该周期是否已迁移完成
                    if self.check_date(table_name, partition_date, error=None):
                        # 返回结果不为空
                        continue

                    # 添加分区
                    self.add_partition(table_name, partition_date)

                    # 单条测试，正式上线后删掉
                    # break
                except Exception as e:
                    print '全量表-出现异常：', table_name, partition_date

    # 检测该周期是否已迁移完成
    def check_date(self, table_name, partition_date, error):
        print '初始化检测，更新同步状态'

        # 检测该表是否存在日志表里，初始化准备
        check_table_sql = "select table_name from tb_copy_data_log where table_name='" + table_name + "\'"

        print '检测该表是否存在:', check_table_sql

        check_table_result = conn_db.select(check_table_sql)

        print '检查结果：', check_table_result
        # 当前时间
        now_time = str(date_time.datetime.now())[0:19]

        # 如果表不存在,返回true，初始化，状态改为正在迁移，执行迁移
        if not check_table_result:

            # 全量表，检测是否在日志表里，日志表里状态是否已同步完成
            if self.table_type == '1':

                if not error:
                    # 初始化mysql同步状态
                    insert_table_sql = "insert into tb_copy_data_log (data_source,table_name,partition_type,partition_time,copy_status,chk_status,start_time,end_time) values('%s','%s','%s','%s','%s','%s','%s','%s')" % (
                        config.data_source, table_name, config.all_table, partition_date, config.copy_status_1,
                        config.chk_status_0, now_time, '0')
                    print '插入记录初始化：', insert_table_sql

                    conn_db.insert(insert_table_sql)

                    # 测试
                    conn_db.insert("insert into test (id) values ('123')")
                    print '完成初始化'

                # 同步失败
                else:
                    # 初始化mysql同步状态
                    insert_table_sql = "insert into tb_copy_data_log (data_source,table_name,partition_type,partition_time,copy_status,chk_status,start_time,end_time) values('%s','%s','%s','%s','%s','%s','%s','%s')" % (
                        config.data_source, table_name, config.all_table, partition_date, config.copy_status_3,
                        config.chk_status_0, now_time, '0')
                    print '插入记录同步失败：', insert_table_sql

                    conn_db.insert(insert_table_sql)

                    # 测试
                    conn_db.insert("insert into test (id) values ('123')")
                    print '完成失败状态同步'


            # 非全量表,todo
            else:
                pass

            return True

        # 表存在日志表里，检测分区，判断是否已迁移
        else:
            # 全量表，检测是否在日志表里，日志表里状态是否已同步完成
            if self.table_type == '1':

                if not error:
                    # 获取迁移状态
                    copy_status_sql = "select * from tb_copy_data_log where table_name='" + table_name + "';"

                    copy_status = conn_db.select(copy_status_sql)

                    # 已同步完成
                    if copy_status[0][0] == '2':
                        return False

                    # 同步失败
                    else:
                        return True
                # 更新失败同步状态
                else:
                    # 更新失败同步状态
                    update_table_sql = "update tb_copy_data_log set copy_status='" + config.copy_status_3 + "' ,end_time='" + str(
                        date_time.datetime.now())[0:19] + "' where table_name='" + table_name + "\'"

                    print '插入记录同步失败：', update_table_sql

                    conn_db.insert(update_table_sql)

                    # 测试
                    conn_db.insert("insert into test (id) values ('123')")
                    print '完成失败状态同步'

            # 非全量表,todo
            else:

                # 检测分区是否已存在
                if not check_table_result:
                    # 初始化mysql同步状态
                    insert_table_sql = "insert into tb_copy_data_log (data_source,table_name,partition_type,partition_time,copy_status,chk_status,start_time,end_time) values('%s','%s','%s','%s','%s','%s','%s','%s')" % (
                        config.data_source, table_name, config.partition_statis_date, partition_date,
                        config.copy_status_1,
                        config.chk_status_0, now_time, '0')
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
                        config.data_source, table_name, config.partition_statis_date, partition_date,
                        config.copy_status_1,
                        config.chk_status_0, now_time, '0')
                    print '插入记录初始化：', insert_table_sql
                    check_table_result = conn_db.insert(insert_table_sql)

                # 检测同步状态
                check_status_sql = " select table_name from tb_copy_data_log where table_name='" + table_name + "' and partition_time= '" + partition_date + "' and copy_status = '" + config.copy_status_0 + "\'"

                print check_status_sql
                select_result = conn_db.select(check_status_sql)

                print '检测结果：', select_result

                # 结果判空
                if select_result:

                    return True
                else:
                    return False

    # 添加分区，清理数据，优化之后不需要添加分区
    def add_partition(self, table_name, partition_date):
        # 清理数据
        truncate_table_sql = "truncate table " + table_name + ';'
        truncate_table_sql_sh = config.excute_hive_sh + truncate_table_sql + "\'"
        print '清理数据', truncate_table_sql_sh
        os.popen(truncate_table_sql_sh)

        # 开始迁移
        self.copy_data(table_name, partition_date)

    # 构造迁移语句
    def copy_data(self, table_name, partition_date):
        # 记录开始迁移时间
        st_time = date_time.datetime.now()
        print "[info]" + str(st_time), ":表数据迁移开始:", table_name, "分区:", partition_date

        # 全量表
        if self.table_type == '1':
            distcp_sh = "hadoop distcp -bandwidth " + self.bandwidth + " -m  " + self.map_num + " -pb -i hdfs://192.168.190.89:8020/apps/hive/warehouse/csap.db/" + table_name + "/* hdfs://172.19.168.4:8020/warehouse/tablespace/managed/hive/csap.db/" + table_name + "/"
            print '全量表-数据迁移命令：', distcp_sh
            os.popen(distcp_sh)

        # 非全量表,todo,分区类型获取
        else:
            # hadoop distcp -i hdfs://192.168.190.89:8020/apps/hive/warehouse/csap.db/tb_si_cu_voma_limit_whitelist_day/statis_date=20170617 hdfs://172.19.168.4:8020/warehouse/tablespace/managed/hive/tb_si_cu_voma_limit_whitelist_day

            distcp_sh = "hadoop distcp -bandwidth " + self.bandwidth + " -m  " + self.map_num + " -pb -i hdfs://192.168.190.89:8020/apps/hive/warehouse/csap.db/" + table_name + "/" + config.partition_statis_date + "=" + partition_date + " hdfs://172.19.168.4:8020/warehouse/tablespace/managed/hive/csap.db/" + table_name + "/"
            # distcp_sh = "hadoop distcp -bandwidth " + self.bandwidth + " -m  " + self.map_num + " -pb -i hdfs://192.168.190.89:8020/apps/hive/warehouse/csap.db/" + table_name + "/* hdfs://172.19.168.4:8020/warehouse/tablespace/managed/hive/csap.db/" + table_name + "/"

            print '非全量表-数据迁移命令：', distcp_sh

            os.popen(distcp_sh)
        end_time = date_time.datetime.now()
        print "[info]" + str(end_time), ":表数据迁移结束:", table_name, "分区:", partition_date
        print '共耗时:', end_time - st_time, 'S'

        # 收集统计信息
        # self.add_info(table_name, partition_date)

        self.copy_ok(table_name, partition_date, st_time, end_time)

    # 数据迁移完成更新数据库记录
    def copy_ok(self, table_name, partition_date, st_time, end_time):
        # 全量表
        if self.table_type == '1':
            update_status_sql = "update tb_copy_data_log set copy_status ='" + config.copy_status_2 + "',start_time='" + str(
                st_time)[0:19] + "',end_time='" + str(end_time)[
                                                  0:19] + "' where table_name='" + table_name + "\'"

            print '更新sql:', update_status_sql

            conn_db.insert(update_status_sql)

        else:

            update_status_sql = "update tb_copy_data_log set copy_status ='" + config.copy_status_2 + "',start_time='" + str(
                st_time)[
                                                                                                                         0:19] + "',end_time='" + str(
                end_time)[0:19] + "' where table_name='" + table_name + "' and partition_time='" + partition_date + "\'"

            print '更新sql:', update_status_sql

            conn_db.insert(update_status_sql)

        print '数据迁移同步,更新同步状态完成:', table_name, partition_date
        if str(date_time.datetime.now())[11:13] == '06':
            print '到达早上6点,自动停止迁移任务,当前时间:', str(date_time.datetime.now())[0:19]
            exit(0)

    # 修复表
    def add_info(self, table_name, partition_date):
        add_info_sql = "ANALYZE TABLE " + table_name + " partition(" + config.partition_statis_date + "= " + partition_date + " ) COMPUTE STATISTICS;"

        add_info_sql_sh = config.excute_hive_sh + add_info_sql + "\'"

        print '收集元数据库统计信息:', add_info_sql_sh

        os.popen(add_info_sql_sh)

        print '[info] ', str(date_time.datetime.now())[0:19], ':收集元数据库统计信息完成：', table_name, '分区：', partition_date


# 全量表
# python copy_data_sy_to_ocdp.py 1 0 20 30

# 启动
if __name__ == '__main__':

    input_length = len(sys.argv)
    print 'input_str: ', len(sys.argv)

    if input_length == 6:

        # 表分类，全量表，日表，月表、年表
        table_type = sys.argv[1]

        # 批次号，分批处理
        input_batch = sys.argv[2]

        # 文件大小，区分文件大小
        size_type = sys.argv[3]

        # 带宽，限制带宽
        bandwidth = sys.argv[4]

        # map数，限制资源
        map_num = sys.argv[5]

        copy_data_object = CopyData(table_type, input_batch, size_type, bandwidth, map_num)
        copy_data_object.read_table_name()

    else:
        print '输入参数有误'
