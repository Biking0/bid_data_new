#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：bdi_monitor_task.py
# 功能描述：监控大数据平台任务积压情况
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20191229
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python code_head.py
# ***************************************************************************

# 程序结构：
#1.生成近5天的running文件
#2.获取无后缀文件数量
#3.生成校验文件上传到hdfs
#4.计算hdfs校验文件并删除校验文件
#5.等待10分钟下次检测

import os
import sys
import time
import config

# days_-1=86400
# days_-2=172800
# days_-3=345600
# days_-4=691200
# days_-5=1382400


# 生成近5天的running文件
def create_running():
	
	# 获取近5天日期变量
	day_input_date=time.strftime('%Y%m%d',time.localtime(int(time.time())-86400))
	
	# 删除校验文件
	#delete_chk_sh='rm *.chk'
	
	# 删除running文件
	delete_running_sh='rm runing.txt'
	
	# 获取近5天日期字符串
	day_0=time.strftime('%Y%m%d',time.localtime(int(time.time())-0))
	day_1=time.strftime('%Y%m%d',time.localtime(int(time.time())-86400))
	day_2=time.strftime('%Y%m%d',time.localtime(int(time.time())-172800))
	day_3=time.strftime('%Y%m%d',time.localtime(int(time.time())-300000))
	day_4=time.strftime('%Y%m%d',time.localtime(int(time.time())-691200))
	#day_-5=time.strftime('%Y%m%d',time.localtime(int(time.time())-day_-4))
	
	day_0_sh='ls /home/bdi/Asiainfo/tas/logs/*'+day_0+'* >> runing.txt'
	day_1_sh='ls /home/bdi/Asiainfo/tas/logs/*'+day_1+'* >> runing.txt'
	day_2_sh='ls /home/bdi/Asiainfo/tas/logs/*'+day_2+'* >> runing.txt'
	day_3_sh='ls /home/bdi/Asiainfo/tas/logs/*'+day_3+'* >> runing.txt'
	#day_0_sh='ll /home/bdi/Asiainfo/tas/logs/*'+day_0+'* >> runing.txt'
	#day_0_sh='ll /home/bdi/Asiainfo/tas/logs/*'+day_0+'* >> runing.txt'
	
	print '生成校验文件命令'
	print day_0_sh
	print day_1_sh
	print day_2_sh
	print day_3_sh
	
	# 生成running文件
	
	os.popen(delete_running_sh)
	os.popen(day_0_sh)
	os.popen(day_1_sh)
	os.popen(day_2_sh)
	os.popen(day_3_sh)
	
def create_chk():
	f=open('runing.txt').readlines()
	
	print type(f)
	a=0
	for i in f:
		if '.log' in i:
			pass
		else :
			a=a+1
			print i
	print f[0]
	print 123
	
	print 'a',a
	
	# 判断是否上传
	check_upload=''
	
	#chk_file=open('119.chk','ab')
	chk_file=open('120.chk','w')
	chk_file.write(str(a))
	
	#time.strftime('%Y%m%d',time.localtime(int(time.time())-345600))
	
	# 生成校验文件上传到hdfs
	
	upload_chkfile_sh=''
	
	# 查询solr将结果输出到search_solr.txt
	#solr_sh='curl -i -H "Content-Type:application/json" -X POST --data \''+'{"condition":"activity_num:'+self.activity_num+' AND start_time:{\\"'+self.start_time_stamp+'\\" TO \\"'+self.end_time_stamp+'\\"}" ,"tables":["DW_LOC_ZX_USER_BH_MIN_20190917"],"query":"","start":0,"return_fields":["phone","imsi","start_time","city_id","county_id"],"cursor_mark":"*","sort":"id desc","rows":10000}\' '+'http://10.218.146.65:9000/ocsearch-service/query/search | more'+' > '+config.log_path+'solr_monitor.txt'
	
	#print solr_sh
	
	#os.popen(solr_sh).readlines()

create_running()
create_chk()
#f=open('runing.txt').readlines()
#
#print type(f)
#a=0
#for i in f:
#	if '.log' in i:
#		pass
#	else :
#		a=a+1
#		print i
#print f[0]
#print 123
#
#print 'a',a
#
##chk_file=open('119.chk','ab')
#chk_file=open('119.chk','w')
#chk_file.write(str(a))

