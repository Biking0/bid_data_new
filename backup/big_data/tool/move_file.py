#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：move_file.py
# 功能描述：clean 60 days ago log
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20200202
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python code_head.py
# ***************************************************************************

import os
import sys
import time

#path="./"

#delete 2 day ago file
delete_day=180
move_day=3

move_time=time.time()-3600*24*move_day

# 需要转移的日志路径
#zx_core='/home/ocdp/javaApp/zx_core_tomcat/logs/'
#zj_interface='/home/ocdp/javaApp/zj_interface_apache-tomcat-7.0.73/logs/'
#zj_task2='/home/ocdp/javaApp/zj_task2_apache-tomcat-7.0.73/logs/'
#zx_web='/home/ocdp/javaApp/zx_web_tomcat/logs/'

# 原日志路径，目标路径
path_list=[
['/home/ocdp/javaApp/zx_core_tomcat/logs','/data/javaAppLogs/zx_core'],
['/home/ocdp/javaApp/zj_interface_apache-tomcat-7.0.73/logs','/data/javaAppLogs/zj_interface'],
['/home/ocdp/javaApp/zj_task2_apache-tomcat-7.0.73/logs','/data/javaAppLogs/zx_task'],
['/home/ocdp/javaApp/zx_web_tomcat/logs','/data/javaAppLogs/zj_web'],

]

# 目标路径

#path_list=['/var/log/hadoop-yarn/ocdp/','/var/log/hadoop/ocdp','/var/log/hbase']
#path_list=[zx_core,zj_interface,zj_task2,zx_web]
# 需要与原日志路径对应
#target_path_list=[]

for path in path_list:
	# 遍历文件
	for file in os.listdir(path[0]):
		
		# 获取文件名称加绝对路径
		filename=path[0]+os.sep+file
		
		# 判断文件有效期
		if os.path.getmtime(filename)< move_time:
				#os.remove(filename)
				move_sh='mv '+filename+' '+path[1]
				print move_sh
				os.popen(move_sh).readlines()
				print filename+" is moved"

