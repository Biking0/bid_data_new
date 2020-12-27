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
import conn_db


def nn_monitor():
    hdfs1_sh = "hdfs haadmin -getServiceState nn1"
    hdfs2_sh = "hdfs haadmin -getServiceState nn2"
    hdfs3_sh = "hdfs haadmin -getServiceState nn3"

    result1 = os.popen(hdfs1_sh).readline().replace('\n', '')
    result2 = os.popen(hdfs2_sh).readline().replace('\n', '')
    result3 = os.popen(hdfs3_sh).readline().replace('\n', '')

    print type(result1), result1
    print type(result2), result2
    print type(result3), result3

    if result1 == '': result1 = '节点异常'
    if result2 == '': result2 = '节点异常'
    if result3 == '': result3 = '节点异常'

    now_time = str(date_time.datetime.now())[0:19]
    print '当前时间：',now_time
    user_list = ['15264899856', '15936466867', '13665656104']
    sms_info_list = []
    # if result1 == '' or result2 == '':
    if result1 == '' or result2 == '':
        sms_info = "namenode服务异常，请及时处理！节点状态: nn1:%s;nn2:%s; 当前时间：%s" % (result1, result2, now_time)

        for i in user_list:
            insert_sql = "insert into tb_sys_sms_send_cur (serv_number,send_date,text,opt_user,area_code,sms_type,plan_id,temp_id) " \
                         "values ('%s','%s','%s','%s','%s','%s','%s','%s') " % \
                         (i, '2020-05-21 16:00:00', sms_info, 'mazhong', '999', '主机告警', 'hourt_alerm',
                          '4649')
            sms_info_list.append(insert_sql)

        print 'sms_info_list:', sms_info_list

        conn_db.insert_batch(sms_info_list)
        # send_sms(sms_info_list)

    # if result3 == '':
    #     print 'nn3 error'


nn_monitor()
