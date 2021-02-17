#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ********************************************************************************
# 文件名称：run_yarn_monitor.py
# 功能描述：报错检测分区
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20200816
# 修改日志：
# 修改日期：
# *******************************************************************************
# 程序调用格式：nohup python run_yarn_monitor.py >> nohup.out &
# *******************************************************************************


import os
import sys
import time
import yarn_monitor
import datetime as date_time

# 启动
if __name__ == '__main__':

    while True:
        # 休息10分钟，600
        yarn_monitor.get_appid()
        print 'sleep 1800s'

        end_time = date_time.datetime.now()
        f = open('./log/kill_log.log', 'a+')
        f.write(str(end_time)[0:19] + ' sleep 1800s \n')

        time.sleep(1800)
