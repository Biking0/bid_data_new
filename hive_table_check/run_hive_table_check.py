# -*-coding:utf-8 -*-
#********************************************************************************
# 文件名称：run_hive_table_check.py
# 功能描述：华为任务监控
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20191020
# 修改日志：20191226
# 修改日期：
# ****************************************************************************
# 程序调用格式：nohup python run_hive_table_check.py >> nohup.out &
# *******************************************************************************

import os
import sys
import time
import hive_table_check

# 每半个小时告警一次
sleep_time=1800

# 启动
if __name__=='__main__':
  
  while True:       

		hive_table_check.main()
	   
		# 休息10分钟，600
		print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime()),'sleep '+str(sleep_time)+'s'
		time.sleep(sleep_time)
