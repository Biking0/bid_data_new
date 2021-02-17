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
import datetime as date_time

from yarn_api_client import ApplicationMaster, HistoryServer, NodeManager, ResourceManager

# 过滤重要任务
ats = 'ats'
thritf = 'Thrift'

# 3个小时之前
# run_time = 10800

# 24小时之前
run_time = 86400


# 获取超时任务id
def get_appid():
    rm = ResourceManager(service_endpoints=['http://172.19.168.100:8088', 'http://172.19.168.4:8088'])

    appid_list = []

    for i in rm.cluster_applications().data.get('apps').get('app'):

        # 过滤已完成状态，保留重要进程
        if i.get('state') <> 'FINISHED' and (not ats in i.get('name')) and (not thritf in i.get('name')):

            start_time = i.get('startedTime') / 1000

            if time.time() - int(start_time) > run_time:
                # print 'time', time.time(), start_time
                appid = i.get('id')
                # print appid, i.get('name'), i.get('startedTime'), i.get('state')

                appid_list.append([appid, i.get('name'), i.get('startedTime'), i.get('state')])

    if appid_list:
        kill_appid(appid_list)


# 杀掉超时任务
def kill_appid(appid_list):
    end_time = date_time.datetime.now()
    f = open('./log/kill_log.log', 'a+')
    for i in appid_list:
        kill_sh = "yarn app -kill " + i[0]
        print 'kill_sh:', kill_sh

        f.write(str(end_time)[0:19] + ' ' + kill_sh + ' ' + str(i[1]) + ' ' + str(i[2]) + ' ' + str(i[3]) + '\n')

        # 杀任务
        os.system(kill_sh)

    f.close()
