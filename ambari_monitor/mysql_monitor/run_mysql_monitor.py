#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：run_mysql_monitor.py
# 功能描述：hive表数据稽核
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20200624
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：nohup python /home/ocdp/hyn/mysql_monitor/run_mysql_monitor.py >> /home/ocdp/hyn/mysql_monitor/nohup.out &
# ***************************************************************************

import os
import sys
import time
import mysql_monitor

# 启动
if __name__ == '__main__':

    while True:
        # 休息10分钟，600
        mysql_monitor.mysql_monitor()
        print 'sleep 300s'

        time.sleep(300)
