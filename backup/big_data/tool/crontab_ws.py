#!/usr/bin/env python
# -*-coding:utf-8 -*-
#********************************************************************************
# ** 文件名称：solr_monitor.py
# ** 功能描述：solr监控，查询最30分钟数据量
# ** 输入参数：
# ** 输 出 表：
# ** 创 建 者：hyn
# ** 创建日期：20191015
# ** 修改日志：
# ** 修改日期：
# *******************************************************************************
# ** 程序调用格式：python solr_monitor.py
# *******************************************************************************
import os
import sys
import time
import json
import datetime

test_sh='nohup sh -x /home/bdi/ws/shell/zp_jg_ws_20200115.sh>./20200116.log 2>&1 &'

# 夜里两点执行
time.sleep(21600)
print '执行脚本'
os.popen(test_sh).readlines()

# ps -ef | grep 20200114.sh

# nohup python  test.py >> nohup_sanqi.out &