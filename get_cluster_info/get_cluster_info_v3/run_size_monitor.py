#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：run_size_monitor.py
# 功能描述：hive表数据稽核
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20200624
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：nohup python /home/ocdp/hyn/disk_monitor/run_disk_monitor.py >> /home/ocdp/hyn/disk_monitor/nuhup.out &
# ***************************************************************************

import os
import sys
import time
import size_monitor

# 启动
if __name__ == '__main__':

    while True:
        # 休息10分钟，600
        size_monitor.size_monitor()
        print 'sleep 600s'

        time.sleep(60)
