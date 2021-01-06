#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：host_monitor.py
# 功能描述：监控资源使用情况
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20210106
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python host_monitor.py
# ***************************************************************************

import os
import sys
import datetime as date_time


def host_monitor():
    sh = 'ps aux|head -1;ps aux|grep -v PID|sort -rn -k +4|head'

    f = open('./host_monitor.log', 'a+')

    now_time = str(date_time.datetime.now())[0:19]
    result = os.popen(sh).readlines()

    for i in result:
        f.write(i)

    f.write('#' + now_time + '\n')
