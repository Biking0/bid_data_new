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
import send_sms

py_path = '/'

ip_list = [
    ['172.19.168.83', 1],
    ['172.19.168.84', 1],
    ['172.19.168.85', 1],
    ['172.19.168.93', 3],
    ['172.19.168.94', 3],
    ['172.19.168.95', 2]
]


def redis_monitor():
    error_list = []

    # 遍历主机ip
    for ip_info in ip_list:
        # os.system("ssh "+ip_list[i]+" df -h" )
        ip = ip_info[0]
        count_sh = "ssh " + ip + " ps -ef | grep redis | grep -v grep | wc -l"

        redis_count = os.popen(count_sh).readline()

        print 'redis_count', redis_count, redis_count

        if not int(redis_count) == ip_info[1]:
            error_list.append(ip_info[0])

    if len(error_list) > 0:
        sms_info = "dataos机器redis进程异常，主机：%s" % (str(error_list)).replace('\'', '')
        print 'sms_info:', sms_info
        send_sms.send_sms(sms_info)
    else:
        print 'dataos机器redis进程正常'

        # 测试
        # break



