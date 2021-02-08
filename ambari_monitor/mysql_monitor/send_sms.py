#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：send_sms.py
# 功能描述：集群磁盘监控，100机器上部署
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20201209
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python send_sms.py
# ***************************************************************************

import config
import datetime as date_time
import conn_db


def send_sms(sms_info):
    now_time = str(date_time.datetime.now())[0:19]
    print '当前时间：', now_time
    user_list = ['15264899856', '15936466867', '13665656104']
    # user_list = ['15936466867', '15264899856', '15939161277']
    # user_list = ['15936466867']
    sms_info_list = []
    sms_info = sms_info + ' 当前时间：' + now_time

    for i in user_list:
        insert_sql = "insert into tb_sys_sms_send_cur (serv_number,send_date,text,opt_user,area_code,sms_type,plan_id,temp_id) " \
                     "values ('%s','%s','%s','%s','%s','%s','%s','%s') " % \
                     (i, '2020-05-21 16:00:00', sms_info, 'mazhong', '999', '主机告警', 'hourt_alerm',
                      '4649')
        sms_info_list.append(insert_sql)

    print 'sms_info_list:', sms_info_list

    conn_db.insert_batch(sms_info_list)
