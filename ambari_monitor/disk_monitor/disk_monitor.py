#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：disk_monitor.py
# 功能描述：集群磁盘监控，100机器上部署
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20201209
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python disk_monitor.py
# ***************************************************************************

import os
import sys
import time
import subprocess
import config
import datetime as date_time
import conn_db
import send_sms

ip_file = open(config.py_path + 'ip_list.txt', 'r')
ip_list = ip_file.readlines()
ip_file.close()


def disk_monitor():
    # 遍历主机ip
    for i in range(len(ip_list)):
        # os.system("ssh "+ip_list[i]+" df -h" )
        ip = ip_list[i].replace('\n', '')
        disk_sh = "ssh " + ip + " df -h"
        # print 'disk_sh', disk_sh
        disk_list = os.popen(disk_sh).readlines()
        # print disk_list

        # 遍历磁盘
        for j in range(1, len(disk_list)):
            # print disk_list[j], '\n'

            disk_size = disk_list[j].replace('\n', '').split(' ')
            # print disk_size

            disk_size_new = [x.strip() for x in disk_size if x.strip() != '']

            # print disk_size_new
            disk_space = int(disk_size_new[4].replace('%', ''))
            if disk_space > 80:
                sms_info = "磁盘空间告警，主机：%s,已用空间%s,挂载分区：%s" % (ip, disk_space, disk_size_new[5])
                print 'sms_info:', sms_info
                send_sms.send_sms(sms_info)

        # 测试
        # break


