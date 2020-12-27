#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：close_lock.py
# 功能描述：主程序
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20200820
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python close_lock.py
# ***************************************************************************

import os
import sys
import config
import datetime as date_time
import conn_db

# 常量定义


# 连接新集群
# new_hive = "beeline -u 'jdbc:hive2://172.19.40.241:10000/csap' -n ocdp -p 1q2w1q@W -e \" "
new_hive = "hive -e \" use csap; "


class Closelock():

    def __init__(self, input_batch):
        self.input_batch = input_batch

    # 获取任务
    def get_task(self):
        while True:
            # 获取可以稽核表名列表，表里获取分区键
            get_task_sql = "select id, table_name ,batch_num from tb_close_lock_get_task where copy_status='0' and   batch_num='" + self.input_batch + "' order by id asc limit 1"

            print '获取任务sql：', get_task_sql

            select_result = conn_db.select(get_task_sql)
            print '获取任务：', select_result

            # 取不到任务
            if not select_result:
                print '无迁移任务'
                exit(0)

            # 遍历集合，更新此批次状态,status=1
            update_sql = "update tb_close_lock_get_task  set copy_status='1'"
            in_condition = ''
            for i in select_result:
                in_condition = str(i[0]) + "," + in_condition

            update_sql = update_sql + "where id in (" + in_condition + "0)"

            print '更新此批次状态', update_sql
            result = conn_db.insert(update_sql)

            # 遍历表名，插日志，开始执行任务
            for i in select_result:
                table_name = i[1]

                # 当前时间
                now_time = str(date_time.datetime.now())[0:19]

                insert_sql = "insert into tb_close_lock_log (table_name,rename_status,create_status,insert_status,start_time,end_time) values('%s','%s','%s','%s','%s','%s')" % (
                    table_name, config.rename_status_0, config.create_status_0, config.insert_status_0,
                    now_time, '')

                result = conn_db.insert(insert_sql)

                self.check_name(table_name)

            print '无任务'

    def check_name(self, table_name):

        new_table_name = table_name + "_bakup"

        check_sql = "desc " + new_table_name

        check_sql_sh = new_hive + check_sql + " \""

        result = os.system(check_sql_sh)

        if result != 0:
            print '### 表不存在'
            self.rename_table(table_name)

        else:
            print '## 表存在'
            self.create_table_like(table_name)

    # 重命名表
    def rename_table(self, table_name):

        print '重命名'
        now_time = date_time.datetime.now()

        # 开始重命名
        update_status_sql = "update tb_close_lock_log set rename_status='" + str(
            config.rename_status_1) + "', start_time='" + str(now_time) + "' where table_name= '" + table_name + "\' "

        print '更新sql:', update_status_sql
        conn_db.insert(update_status_sql)

        new_table_name = table_name + "_bakup"

        rename_sql = "ALTER TABLE " + table_name + " RENAME TO " + new_table_name + ";"

        rename_sql_sh = new_hive + rename_sql + "\""

        print 'rename_sql_sh', rename_sql_sh

        result = os.system(rename_sql_sh)

        if result != 0:
            # 失败
            print '### error'
            update_status_sql = "update tb_close_lock_log set rename_status='" + str(
                config.rename_status_3) + "', start_time='" + str(
                now_time) + "' where table_name= '" + table_name + "\' "
            print '更新sql:', update_status_sql
            conn_db.insert(update_status_sql)

        else:
            # 成功
            update_status_sql = "update tb_close_lock_log set rename_status='" + str(
                config.rename_status_2) + "', start_time='" + str(
                now_time) + "' where table_name= '" + table_name + "\' "
            print '更新sql:', update_status_sql
            conn_db.insert(update_status_sql)

            self.create_table_like(table_name)

    # 创建新表
    def create_table_like(self, table_name):

        print '### 创建新表'
        update_status_sql = "update tb_close_lock_log set create_status='" + str(
            config.create_status_1) + "' where table_name= '" + table_name + "\' "
        print '更新sql:', update_status_sql
        conn_db.insert(update_status_sql)

        new_table_name = table_name + "_bakup"
        create_table_like_sql = "create table " + table_name + " like " + new_table_name

        create_table_like_sql_sh = new_hive + create_table_like_sql + '\"'

        print 'create_table_like_sql_sh', create_table_like_sql_sh

        result = os.system(create_table_like_sql_sh)

        if result != 0:
            # 失败
            print '### error'
            update_status_sql = "update tb_close_lock_log set create_status='" + str(
                config.create_status_3) + "' where table_name= '" + table_name + "\' "
            print '更新sql:', update_status_sql
            conn_db.insert(update_status_sql)

        else:
            # 成功
            update_status_sql = "update tb_close_lock_log set create_status='" + str(
                config.create_status_2) + "' where table_name= '" + table_name + "\' "
            print '更新sql:', update_status_sql
            conn_db.insert(update_status_sql)

        self.copy_data(table_name)

    # 复制数据
    def copy_data(self, table_name):

        print '### 复制数据'
        update_status_sql = "update tb_close_lock_log set insert_status='" + str(
            config.insert_status_1) + "' where table_name= '" + table_name + "\' "
        print '更新sql:', update_status_sql
        conn_db.insert(update_status_sql)

        new_table_name = table_name + "_bakup"

        insert_sql = "insert overwrite table " + table_name + " select * from " + new_table_name

        insert_sql_sh = new_hive + insert_sql + "\""

        print 'insert_sql_sh: ', insert_sql_sh

        result = os.system(insert_sql_sh)

        if result != 0:
            # 失败
            print '### error'
            update_status_sql = "update tb_close_lock_log set insert_status='" + str(
                config.insert_status_3) + "' where table_name= '" + table_name + "\' "
            print '更新sql:', update_status_sql
            conn_db.insert(update_status_sql)

        else:
            # 成功
            update_status_sql = "update tb_close_lock_log set insert_status='" + str(
                config.insert_status_2) + "' where table_name= '" + table_name + "\' "
            print '更新sql:', update_status_sql
            conn_db.insert(update_status_sql)

            # self.repair_table(table_name)

    # 修复表
    def repair_table(self, table_name):
        repair_table_sql = "msck repair table " + table_name

        add_partition_sql_sh = new_hive + repair_table_sql + "\""

        os.popen(add_partition_sql_sh)


# 启动入口
if __name__ == '__main__':

    # 输入表明参数处理
    input_length = len(sys.argv)
    print 'input_str: ', len(sys.argv)

    monitor_server = 1
    if input_length == 2:
        close_lock = Closelock(sys.argv[1])
        close_lock.get_task()

    else:
        print '输入表名参数'
