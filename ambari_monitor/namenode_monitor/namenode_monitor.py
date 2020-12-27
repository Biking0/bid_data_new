#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：copy_hive_table.py
# 功能描述：迁移Hive表
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20201028
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python copy_hive_table.py
# ***************************************************************************

import os
import datetime as date_time


# 获取历史状态
def get_old_status():
    f = open('./nn_info.txt', 'r+')
    nn_info_list = f.readlines()

    return nn_info_list





def nn_monitor(nn_info_list):



    # 判空
    if len(nn_info_list) == 0:

        print '# 未保存状态'
        return False
    else:
        print '# 获取到历史状态'



    # 获取旧状态

    hdfs1_sh = "hdfs haadmin -getServiceState nn1 >> ./nn_info.txt"
    hdfs2_sh = "hdfs haadmin -getServiceState nn2 >> ./nn_info.txt"
    hdfs3_sh = "hdfs haadmin -getServiceState nn3 >> ./nn_info.txt"

    f = open('./nn_info.txt', 'r')

    nn_info_list = f.readlines()

    f.truncate()
    f.close()

    result = os.popen(hdfs_sh).readline()
    print result
