#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：clean_kafka_log.py
# 功能描述：65机器有kafka集群的互信，该脚本放于65机器批量删除kafka日志，
#           只保留最近两天的日志
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20191010
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：clean_kafka_log.py
# ***************************************************************************

import os
import sys
import time

path="/var/log/kafka"

#delete 2 day ago file
day = 2

delete_time=time.time()-3600*24*day

for file in os.listdir(path):
        filename=path+os.sep+file

        # 保留20个文件，防止日志全删
        if len(os.listdir(path))<21:
                break

        if os.path.getmtime(filename)< delete_time:
                os.remove(filename)
                print filename+" is removed"