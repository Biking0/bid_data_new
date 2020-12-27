#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：cpu_monitor.py
# 功能描述：
# 1.监控华为yarn资源
# 2.监控119、120主机硬盘空间
# 功能描述：
# 功能描述：
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20191023
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python cpu_monitor.py
# ***************************************************************************

import os
import sys
import time
import json
import datetime

result_list=[]

from yarn_api_client import ApplicationMaster,HistoryServer,NodeManager,ResourceManager

#rm =ResourceManager(address='10.93.171.97',port='8088')
#rm =ResourceManager(service_endpoints='10.93.171.97',port='8088')
rm =ResourceManager(service_endpoints='10.93.171.97:8088')
print (rm.cluster_applications().data)

