#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：cpu_monitor.py
# 功能描述：
# 1.监控华为yarn资源
# 2.监控119、120主机硬盘空间
# 功能描述：
# 功能描述：
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20191023
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python cpu_monitor.py
# ***************************************************************************

import os
import sys
import time
import json
import datetime

# cpu使用告警值
cpu_num=1000

# HDFS通知文件目录
hdfs_path='/asiainfo/dependent/sms_info/'

# 文件移动追加时间，防止文件重复
file_time=time.strftime('%Y%m%d%H%M%S',time.localtime())
info_filename='job_list_'+file_time+'.txt'

# 将job信息输出到文件
def top():
	#top_sh="hadoop job -list | grep -E \"RUNNING|PREP\"|awk -F\"\t\" '{print $2,$8}' > job_list.txt"
	#top_sh="hadoop job -list | grep -E \"RUNNING|PREP\"|awk -F\" \" '{print $1\"--\"$2\"--\"$3\"--\"$7\"--\"$8\"--\"$9\"--\"$10}' > job_list.txt"
	#top_sh="hadoop job -list | grep -E \"RUNNING|PREP\"|awk -F\"\t\" '{print $2,$8}' > job_list.txt"
	top_sh="hadoop job -list | grep -E \"RUNNING|PREP\"|awk -F\" \" '{ if( $8>"+str(cpu_num)+" ) print $2}' > "+info_filename
	os.popen(top_sh).readlines()
	
	print top_sh
	
	parser_file()
	
# 解析文件
def parser_file():
	#pass
	
	f=open('./'+info_filename).readlines()
	
	test_str=f
	print f	
	
	result_str=""	
	for i in range(len(f)):
	#for i in f:
		print f[i]
		
		if i == 0: 
			result_str=f[i].replace('\n','')
		else :
			result_str=result_str+','+f[i].replace('\n','')
	
	#告警时间
	now_time=' 告警时间：'+time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())
	
	
	sms_info='bdi任务cpu使用超过'+str(cpu_num)+'个：'+result_str+now_time
	print sms_info
	
	f=open('./'+info_filename,'w')
	
	f.write(sms_info)
	f.close()
	
	print test_str
	print type(test_str)
	
	if len(test_str) > 0 :
		upload_file()
	else:
		print 'bdi资源正常'
		
		delete_file()

# 短信内容文件上传到HDFS通知文件目录
def upload_file():
	upload_sh="hadoop fs -put ./"+info_filename+' '+hdfs_path
	os.popen(upload_sh).readlines()
	
	print '已上传文件'
	
	delete_file()

# 删除本地文件
def delete_file():
	
	delete_sh='rm ./job_list*'
	os.popen(delete_sh).readlines()




