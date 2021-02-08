#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：mysql_monitor.py
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
import sys
import time
import subprocess
import datetime as date_time
import conn_db
import send_sms

error_num = 1000


def mysql_monitor():
    dev_sh = "mysql -h ritdsdataos.mysql.svc.huaarmcore.hpc -P20001 -udataos_dev  -s -N  -pqXliH9*Ro#qDGomY dataos_dev -e \"show status like 'Threads_connected%'; \""
    pro_sh = "mysql -hritdsdataos.mysql.svc.huaarmcore.hpc -P20001 -udataos_pro  -s -N  -Ddataos_pro -pJklCTLKsF7KiW9YF -e \"show status like 'Threads_connected%'; \""

    print 'sh:', dev_sh
    print 'sh:', pro_sh
    now_time = str(date_time.datetime.now())[0:19]
    print now_time

    # dev_num = os.popen(dev_sh).readline().split(' ')
    # pro_num = os.popen(pro_sh).readline().split(' ')

    dev_num = os.popen(dev_sh).readline().split('\t')[1].replace('\n', '')
    pro_num = os.popen(pro_sh).readline().split('\t')[1].replace('\n', '')

    print 'num:', dev_num, pro_num

    error_list = []
    if int(dev_num) > error_num:
        error_list.append('dataos_dev connections ' + dev_num)

    if int(pro_num) > error_num:
        error_list.append('dataos_pro connections ' + dev_num)

    # 触发警告
    if error_list:
        print '触发警告'
        send_info = "dataos元数据连接数警告：" + str(error_list).replace('\'', '')

        send_sms.send_sms(send_info)
    error_list = []

# mysql_monitor()
