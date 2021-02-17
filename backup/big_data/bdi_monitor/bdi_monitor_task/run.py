#!/usr/bin/env python
# -*-coding:utf-8 -*-
#********************************************************************************
# 文件名称：run.py
# 功能描述：华为任务监控
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20191020
# 修改日志：
# 修改日期：20200320
# *******************************************************************************
# 程序调用格式：nohup python run.py >> nohup.out &
# *******************************************************************************


import os
import sys
import time
import config
import bdi_monitor_task


# 启动
if __name__=='__main__':

	while True:		
		
		bdi_monitor_task.create_running()
		
		# 休息10分钟，600
		
		print 'sleep 600s'
		time.sleep(900)

