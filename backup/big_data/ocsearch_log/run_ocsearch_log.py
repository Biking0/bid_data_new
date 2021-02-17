# -*-coding:utf-8 -*-
#!/usr/bin/env python
#********************************************************************************
# 文件名称：run_ocsearch.py
# 功能描述：华为任务监控
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20200326
# 修改日志：
# 修改日期：
# ****************************************************************************
# 程序调用格式：nohup python run_ocsearch.py >> nohup.out &
# *******************************************************************************

import os
import sys
import time

# 每半个小时告警一次
sleep_time=1800

# 启动
if __name__=='__main__':	
	

	while True:	
		
		run_sh='sh check_search_log.sh'
		
		os.popen(run_sh).readlines()
		
		# 休息10分钟，600
		
		print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime()),'sleep '+str(sleep_time)+'s'
		time.sleep(sleep_time)

