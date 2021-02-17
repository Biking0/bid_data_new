#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：yarn_monitor.py
# 功能描述：yarn任务监控
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20200917
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python yarn_monitor.py
# ***************************************************************************

import os
import sys
import json
import time

from yarn_api_client import ApplicationMaster, HistoryServer, NodeManager, ResourceManager

rm = ResourceManager(service_endpoints=['http://172.19.168.100:8088', 'http://172.19.168.4:8088'])
# print rm.cluster_information().data
#
# ApplicationMaster()
#
# NodeManager.

# 过滤重要任务
ats = 'ats'
thritf = 'Thrift'
dis='dis'

# 3个小时之前
# run_time = 10800

# 24小时之前
run_time = 86400

# print rm.cluster_applications().data.get('apps').get('app')[0].get('finalStatus')

for i in rm.cluster_applications().data.get('apps').get('app'):

    # 过滤已完成状态，重要进程
    if i.get('state') <> 'FINISHED'  and ( dis in i.get('name')):

        start_time = i.get('startedTime') / 1000

        if time.time() - int(start_time) > run_time:
            print 'time', time.time(), start_time
            print i.get('id'), i.get('name'), i.get('startedTime'), i.get('state')
