#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：run_cpu_monitor.py
# 功能描述：python脚本头
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20200407
# 修改日志：增大间隔
# 修改日期：20200419
# ***************************************************************************
# 程序调用格式：nohup python run_cpu_monitor.py >> nohup.out &
# ***************************************************************************

# 启动脚本，每个5分钟扫描文件夹
# 调用短信发送脚本
# 将已发送过的文件移到已发送文件夹bak

import os
import sys
import time
import cpu_monitor


# 每半个小时告警一次
sleep_time=1800

# 启动
if __name__=='__main__':

	while True:
		
		cpu_monitor.top()
		
		print('sleep '+str(sleep_time)+'s')
		time.sleep(sleep_time)
