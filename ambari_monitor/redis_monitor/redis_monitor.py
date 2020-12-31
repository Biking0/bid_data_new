#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：redis_monitor.py
# 功能描述：集群磁盘监控，100机器上部署
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20201231
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python redis_monitor.py
# ***************************************************************************

import os
import sys
import time
import subprocess
import datetime as date_time
import conn_db
import send_sms

py_path = './'

ip_list = [
    ['172.19.168.83', 1],
    ['172.19.168.84', 1],
    ['172.19.168.85', 1],
    ['172.19.168.93', 3],
    ['172.19.168.94', 3],
    ['172.19.168.95', 2]
]


def disk_monitor():
    # 遍历主机ip
    for i in range(len(ip_list)):
        # os.system("ssh "+ip_list[i]+" df -h" )
        ip = ip_list[i][0]
        count_sh = "ssh " + ip + " ps -ef | grep redis | grep -v grep | wc -l"
        # print 'disk_sh', disk_sh
        redis_count = os.popen(count_sh).readline()
        # print disk_list



        if redis_count ==
            disk_size = disk_list[j].replace('\n', '').split(' ')
            # print disk_size

            disk_size_new = [x.strip() for x in disk_size if x.strip() != '']

            # print disk_size_new
            disk_space = int(disk_size_new[4].replace('%', ''))
            if disk_space > 70:
                sms_info = "磁盘空间告警，主机：%s,已用空间%s,挂载分区：%s" % (ip, disk_space, disk_size_new[5])
                print 'sms_info:', sms_info
                send_sms.send_sms(sms_info)

        # 测试
        # break
