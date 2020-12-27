#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：run_send_sms.py
# 功能描述：python脚本头
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20200416
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：nohup python run_send_sms.py >> nohup.out &
# ***************************************************************************

# 启动脚本，每个5分钟扫描文件夹
# 调用短信发送脚本
# 将已发送过的文件移到已发送文件夹bak

import os
import sys
import time
import main

# 启动
if __name__=='__main__':
	
	

	while True:
		
		#run_sh='python cpu_monitor.py'
		
		#os.popen(run_sh).readlines()
		main.main()
		print('sleep 120s')
		time.sleep(120)
		
		#time.sleep(3)

