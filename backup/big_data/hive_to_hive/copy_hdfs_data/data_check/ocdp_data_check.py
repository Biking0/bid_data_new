#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：ocdp_data_check.py
# 功能描述：迁移Hive表
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20201014
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python ocdp_data_check.py
# ***************************************************************************

# 1.配置表获取表名作为任务，更新稽核状态，支持并发，带有批次号
# 2.拼接稽核sql，连接hive，将结果输入到文件
# 3.读取文件结果，遍历日志表，表名、周期、库名作为唯一性，存在删除新增，不存在新增，保持最新

import os
import sys
from datetime import datetime
import datetime as date_time
import conn_db
import config


class CheckData():

    # 参数初始化
    def __init__(self, batch_num):
        self.batch_num = batch_num
        self.conn_hive_sh = config.ocdp_hive_sh
        self.db_name = config.ocdp_db_name
        self.file_path = './table_info_ocdp/'
        self.check_status='check_status_ocdp'

    # 获取任务，mysql获取表名，每次获取一个列表进行遍历
    def read_table_name(self):

        while True:
            # 获取可以稽核表名列表，表里获取分区键，只稽核分区表
            get_task_sql = "select id, table_name ,partition_type from tb_copy_data_count_check_task where  "+self.check_status+"='0' and   batch_num ='" + self.batch_num + "'  limit 10"

            print '获取任务sql：', get_task_sql

            select_result = conn_db.select(get_task_sql)
            print '获取任务：', select_result

            # 取不到任务
            if not select_result:
                print '无稽核任务'
                exit(0)

            # 遍历集合，更新此批次状态,status=1
            update_sql = "update tb_copy_data_count_check_task  set %s='1'" % (self.check_status)
            in_condition = ''
            for i in select_result:
                in_condition = str(i[0]) + "," + in_condition

            update_sql = update_sql + "where id in (" + in_condition + "0)"

            print '更新此批次状态', update_sql
            result = conn_db.insert(update_sql)

            # 遍历表名，插日志
            for i in select_result:
                id, table_name, partition_type = i[0], i[1], i[2]

                count_sql = "select %s,count(*) from %s group by %s " % (partition_type, table_name, partition_type)

                print 'count_sql', count_sql

                # 获取稽核结果
                self.get_count_data(count_sql, id, table_name, partition_type)


    # 连接hive，获取结果
    def get_count_data(self, count_sql, id, table_name, partition_type):

        get_count_sh = self.conn_hive_sh + count_sql + '\' > %s%s.txt' % (self.file_path, table_name)

        print 'get_count_sh', get_count_sh

        result = os.system(get_count_sh)

        check_error = ''

        # 同步失败，更新数据库
        if result != 0:
            # 稽核失败，更新配置表，返回
            check_error = '1'
            update_task_error_sql = "update tb_copy_data_count_check_task set check_status=%s where id ='%s'" % (
                config.check_status_2, id)
            print 'update_task_error_sql:', update_task_error_sql

            # 失败更新mysql
            conn_db.insert(update_task_error_sql)
            return

        else:
            # 稽核成功
            check_error = '0'

            self.insert_data(id, table_name, partition_type)

    # 遍历结果文件，插入数据库
    def insert_data(self, id, table_name, partition_type):

        # 清理数据
        self.check_exists(table_name)

        count_result = open(self.file_path + table_name + '.txt', 'r').readlines()

        for i in count_result:
            table_info = i.replace('\n', '').split('\t')
            partition_time, count_num = table_info[0], table_info[1]

            insert_sql = "insert into tb_copy_data_count_check_log (table_name,partition_type,partition_time,db_name,count_num,update_time)  values ('%s','%s','%s','%s','%s',now())" % (
                table_name, partition_type, partition_time, self.db_name, count_num)
            # print 'insert_sql:', insert_sql
            conn_db.insert(insert_sql)

            # 测试
            # break

    # 检测是否存在
    def check_exists(self, table_name):
        # 如果存在先删除，表名，周期，库名
        check_sql = "select id from tb_copy_data_count_check_log where table_name='%s' and db_name='%s'" % (
            table_name, self.db_name)

        print 'check_sql:', check_sql

        check_result = conn_db.select(check_sql)

        print 'check_result:', check_result

        # 存在
        if check_result:
            condition = ''
            if len(check_result) == 1:
                condition = "'%s'" % (check_result[0])
            else:
                for j in check_result:
                    condition = condition + ',' + "'%s'" % (j[0])

            if condition[0] == ',':
                condition = condition[1:]

            print 'condition:', condition

            delete_sql = "delete from tb_copy_data_count_check_log where id in (%s)" % (condition)

            print 'delete_sql:', delete_sql

            conn_db.insert(delete_sql)


# 全量表
# python copy_data_sy_to_ocdp.py 1 0 20 30

# 启动
if __name__ == '__main__':

    input_length = len(sys.argv)
    print 'input_str: ', len(sys.argv)

    if input_length == 2:

        # 批次号，分批处理
        batch_num = sys.argv[1]

        check_data_object = CheckData(batch_num)
        check_data_object.read_table_name()

        # 测试
        # check_data_object.insert_data('1', 'tb_dim_ct_lvcs_menu_day', 'statis_date')

    else:
        print '输入参数有误'
