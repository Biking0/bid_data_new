#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：copy_data_sy_to_ocdp.py
# 功能描述：迁移Hive表
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20200906
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：# python copy_hdfs_data_tool.py table_name 20200911
# ***************************************************************************

import os
import sys
from datetime import datetime
import datetime as date_time

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
    def __init__(self, table_name, partition_date):
        self.table_name = table_name
        self.partition_date = partition_date
        self.partition_type = 'statis_date'

        self.bandwidth = '50'
        self.map_num = '11'

    # 主函数
    def main(self):

        self.add_partition()

    # 添加分区，清理数据，优化之后不需要添加分区
    def add_partition(self):
        # 重建分区
        delete_partition_sql = "alter table " + self.table_name + " drop if  exists partition (" + self.partition_type + "=" + self.partition_date + ");"
        delete_partition_sql_sh = config.excute_hive_sh + delete_partition_sql + "\'"

        # 先测试单周期
        add_partition_sql = "alter table " + self.table_name + " add if not exists partition (" + self.partition_type + "=" + self.partition_date + ");"

        print add_partition_sql

        add_partition_sql_sh = config.excute_hive_sh + add_partition_sql + "\'"

        print '分区已添加：', delete_partition_sql_sh, add_partition_sql_sh
        os.popen(delete_partition_sql_sh)
        os.popen(add_partition_sql_sh)

        self.copy_data()

    # 构造迁移语句
    def copy_data(self):
        # 记录开始迁移时间
        st_time = date_time.datetime.now()
        print "[info]" + str(st_time), ":表数据迁移开始:", self.table_name, "分区:", self.partition_date

        # hadoop distcp -i hdfs://192.168.190.89:8020/apps/hive/warehouse/csap.db/tb_si_cu_voma_limit_whitelist_day/statis_date=20170617 hdfs://172.19.168.4:8020/warehouse/tablespace/managed/hive/tb_si_cu_voma_limit_whitelist_day

        distcp_sh = "hadoop distcp -bandwidth " + self.bandwidth + " -m  " + self.map_num + " -pb -i hdfs://192.168.190.89:8020/apps/hive/warehouse/csap.db/" + self.table_name + "/" + self.partition_type + "=" + self.partition_date + " hdfs://172.19.168.100:8020/warehouse/tablespace/managed/hive/csap.db/" + self.table_name + "/"
        # distcp_sh = "hadoop distcp -bandwidth " + self.bandwidth + " -m  " + self.map_num + " -pb -i hdfs://192.168.190.89:8020/apps/hive/warehouse/csap.db/" + table_name + "/* hdfs://172.19.168.4:8020/warehouse/tablespace/managed/hive/csap.db/" + table_name + "/"

        print '#迁移命令:', distcp_sh

        result = os.system(distcp_sh)

        print '###################'

        print '##### 迁移信息', result, type(result)

        print '###################'

        end_time = date_time.datetime.now()

        # 同步失败，更新数据库
        if result != 0:
            update_status_sql = "update tb_copy_data_log set remark1='" + str(
                result) + "', copy_status ='" + config.copy_status_3 + "',start_time='" + str(
                st_time)[
                                                                                          0:19] + "',end_time='" + str(
                end_time)[0:19] + "' where table_name='" + table_name + "' and partition_time='" + partition_date + "\'"

            print '更新sql:', update_status_sql

            return
        else:

            print "[info]" + str(end_time), ":表数据迁移结束:", table_name, "分区:", partition_date
            print '共耗时:', end_time - st_time, 'S'

            self.copy_ok(table_name, partition_date, st_time, end_time)

    # 数据迁移完成更新数据库记录
    def copy_ok(self, table_name, partition_date, st_time, end_time):
        update_status_sql = "update tb_copy_data_log set copy_status ='" + config.copy_status_2 + "',start_time='" + str(
            st_time)[
                                                                                                                     0:19] + "',end_time='" + str(
            end_time)[0:19] + "' where table_name='" + table_name + "' and partition_time='" + partition_date + "\'"

        print '更新sql:', update_status_sql

        # 修复表，停止修复表
        self.repair_table(table_name)

        # 收集统计信息
        self.add_info()

        # print '数据迁移同步,更新同步状态完成:', table_name, partition_date
        # if str(date_time.datetime.now())[11:13] == '06':
        #     print '到达早上6点,自动停止迁移任务,当前时间:', str(date_time.datetime.now())[0:19]
        #     exit(0)

    # 收集表统计信息
    def add_info(self):
        add_info_sql = "ANALYZE TABLE " + self.table_name + " partition(" + self.partition_type + "=" + self.partition_date + " ) COMPUTE STATISTICS;"

        add_info_sql_sh = config.excute_hive_sh + add_info_sql + "\'"

        print '收集元数据库统计信息:', add_info_sql_sh

        os.popen(add_info_sql_sh)

        print '[info] ', str(date_time.datetime.now())[
                         0:19], ':收集元数据库统计信息完成：', self.table_name, '分区：', self.partition_date

    # 修复表
    def repair_table(self, table_name):
        repair_table_sql = "msck repair table " + table_name

        add_partition_sql_sh = config.excute_hive_sh + repair_table_sql + "\'"

        os.popen(add_partition_sql_sh)


# 全量表
# python copy_data_sy_to_ocdp.py 表名加分区

# 启动
if __name__ == '__main__':

    input_length = len(sys.argv)
    print 'input_str: ', len(sys.argv)

    if input_length == 3:

        # 表名
        table_name = sys.argv[1]

        # 分区时间
        partition_date = sys.argv[2]

        copy_data_object = CopyData(table_name, partition_date)
        copy_data_object.main()

    else:
        print '输入参数有误'
