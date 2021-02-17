#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：clean_zj_log.py
# 功能描述：clean 60 days ago log
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20200202
# 修改日志：清理solr日志
# 修改日期：20200329
# 位置：65:~/hyn
# ***************************************************************************
# 程序调用格式：python clean_zj_log.py
# ***************************************************************************

import os
import sys
import time
import shutil

#delete 2 day ago file
day=10

move_time=time.time()-3600*24*day

#source_path_list=['/home/ocdp/hyn/clean_file/test/test1']
source_path_list=['/data/javaAppLogs/zx_execute','/home/ocdp/javaApp/zj_execute_tomcat7/logs','/home/ocdp/javaApp/zj_interface_apache-tomcat-7.0.73/logs','/home/ocdp/javaApp/zx_core_tomcat/logs']
#target_path_list=['/home/ocdp/hyn/clean_file/test/test2']
target_path_list=['/hdfs/data04/javaAppLogs/zx_execute','/hdfs/data04/javaAppLogs/zx_execute','/hdfs/data04/javaAppLogs/zj_interface','/hdfs/data04/javaAppLogs/zx_core']


# kafka日志不删，'/var/log/kafka'
#'/home/ocdp/javaApp/zx_core_tomcat/logs','/data/javaAppLogs/zx_core','/home/ocdp/javaApp/zj_interface_apache-tomcat-7.0.73/logs','/data/javaAppLogs/zj_interface','/home/ocdp/javaApp/zj_task2_apache-tomcat-7.0.73/logs','/data/javaAppLogs/zx_task','/home/ocdp/javaApp/zx_web_tomcat/logs','/data/javaAppLogs/zj_web','/data']

# 遍历需要清理文件路径
for i in range(len(source_path_list)):
	try:
		# 遍历路径下载文件
		for file in os.listdir(source_path_list[i]):
				filename=source_path_list[i]+os.sep+file
				
				# 判断时间是否过期
				if os.path.getmtime(filename)< move_time:						
						
						# 删除文件
						#os.remove(filename)
						shutil.move(filename,target_path_list[i])
						print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime()),filename+" is moved  to ", target_path_list[i]

						
	except Exception as e:
		
		# 出现异常，继续循环
		print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime()),e
		continue


