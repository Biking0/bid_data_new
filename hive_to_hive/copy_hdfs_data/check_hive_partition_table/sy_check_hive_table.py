#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：sy_check_hive_table.py
# 功能描述：迁移Hive表
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20201014
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：nohup python sy_check_hive_table.py 1 &
# ***************************************************************************

import os
import sys
from datetime import datetime
import datetime as date_time
import conn_db
import config
import time


class CheckData():

    # 参数初始化
    def __init__(self, batch_num):
        self.batch_num = batch_num
        self.conn_hive_sh = config.ocdp_hive_sh
        self.db_name = config.sy_db_name
        self.file_path = './table_info_ocdp/'
        self.count_col = 'sy_count'
        self.update_sql_list = []
        self.start_time = ''
        self.end_time = ''

    # 获取任务，mysql获取表名，每次获取一个列表进行遍历
    def read_table_name(self):

        while True:
            # 获取可以稽核表名列表，表里获取分区键，只稽核分区表
            # get_task_sql = "select id, table_name ,partition_type from tb_copy_data_count_check_task where  " + self.check_status + "='0' and   batch_num ='" + self.batch_num + "'  limit 10"

            # get_task_sql = "select id,table_name,partition_type,partition_time  from tb_copy_data_log where remark3 is null and start_time >'2020-08-13 10:00:51' and copy_status='2' and (%s is  NULL or %s ='')  order by partition_time desc,start_time asc limit 1000;" % (
            #             #     self.count_col, self.count_col)

            get_task_sql = "select id,table_name,partition_type,partition_time  from tb_copy_data_log where remark3 is null and start_time >'2020-08-13 10:00:51' and copy_status='2' and (%s is  NULL or %s ='')  limit 1000;" % (
                self.count_col, self.count_col)

            print '获取任务sql：', get_task_sql

            select_result = conn_db.select(get_task_sql)
            # print '获取任务：', select_result

            # 取不到任务
            if not select_result:
                print '无稽核任务'
                exit(0)

            # # 遍历集合，更新此批次状态,status=1
            update_status_list=[]
            update_sql = "update tb_copy_data_log  set remark3='checked' "
            in_condition = ''
            for i in select_result:
                in_condition = str(i[0]) + "," + in_condition

            update_sql = update_sql + "where id in (" + in_condition + "0)"

            # print '更新此批次状态', update_sql
            result = conn_db.insert(update_sql)

            self.start_time = time.time()
            # 遍历表名，插日志
            for i in select_result:
                id, table_name, partition_type, partition_time = i[0], i[1], i[2], i[3]

                # count_sql = "select %s,count(*) from %s group by %s " % (partition_type, table_name, partition_type)
                count_sql = "select partition_type,partition_time,count_num from tb_copy_data_count_check_log where db_name='%s' and table_name='%s' and partition_time='%s' " % (
                    self.db_name, table_name, partition_time)

                # print 'count_sql', count_sql

                # 获取稽核结果
                self.get_count_data(count_sql, id, table_name, partition_type, partition_time)

            # 更新mysql
            # print 'update_sql_list', self.update_sql_list
            conn_db.insert_batch(self.update_sql_list)

            print 'sleep 5s,更新数据量:', len(self.update_sql_list), '耗时:', time.time() - self.start_time
            self.update_sql_list = []

            time.sleep(3)

    # 连接hive，获取结果
    def get_count_data(self, count_sql, id, table_name, partition_type, partition_time):

        # mysql获取结果
        result = conn_db.select(count_sql)

        check_error = ''

        # 同步失败，更新数据库
        if not result:
            # 稽核失败，更新配置表，返回

            # print '不存在该表',id, table_name, partition_type, partition_time

            return

        else:
            # print '表存在',id, table_name, partition_type, partition_time
            self.insert_data(id,result)

    # 遍历结果文件，插入数据库
    def insert_data(self, id,  result):

        # 更新整张表
        for i in result:
            partition_type, partition_time, count_num = i[0], i[1], i[2]
            update_sql = "update tb_copy_data_log set  %s='%s' where id ='%s' " % (self.count_col, count_num, id)

            self.update_sql_list.append(update_sql)



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
