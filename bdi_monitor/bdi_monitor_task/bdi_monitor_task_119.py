#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：bdi_monitor_task_119.py
# 功能描述：监控大数据平台任务积压情况
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20191229
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python bdi_monitor_task_119.py
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
import datetime
import config

# days_-1=86400
# days_-2=172800
# days_-3=345600
# days_-4=691200
# days_-5=1382400


# 生成近5天的running文件
def create_running():
	
	# 删除校验文件
	#delete_chk_sh='rm *.chk'
	
	# 删除running文件
	delete_running_sh='rm runing.txt'
	
	# 获取近5天日期字符串
	#day_1=time.strftime('%Y%m%d',time.localtime(int(time.time())-86400))
	today=date_test=datetime.datetime.today()
	
	offset_0=datetime.timedelta(days=-0)
	offset_1=datetime.timedelta(days=-1)
	offset_2=datetime.timedelta(days=-2)
	offset_3=datetime.timedelta(days=-3)
	offset_4=datetime.timedelta(days=-4)
	offset_5=datetime.timedelta(days=-5)
	
	day_0=(today+offset_0).strftime('%Y%m%d')
	day_1=(today+offset_1).strftime('%Y%m%d')
	day_2=(today+offset_2).strftime('%Y%m%d')
	day_3=(today+offset_3).strftime('%Y%m%d')
	
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
	
	create_chk()
	
def create_chk():
	f=open('runing.txt').readlines()
	
	print type(f)
	a=0
	for i in f:
		if '.log' in i:
			pass
		else :
			a=a+1
			# print i
	print f[0]
	print 123
	
	print 'a',a
	
	#chk_file=open('119.chk','ab')
	chk_file=open('119.chk','w')
	chk_file.write(str(a))
	chk_file.close()
	
	#time.strftime('%Y%m%d',time.localtime(int(time.time())-345600))
	
	# 判断是否上传校验文件，有119不传，有120计算清空，
	upload_flag=False
	check_119chk_sh='hadoop fs -ls /user/hive/warehouse/asiainfoh.db/bdi_monitor/119chk > check_upload_119.txt'
	check_120chk_sh='hadoop fs -ls /user/hive/warehouse/asiainfoh.db/bdi_monitor/120chk > check_upload_120.txt'
	
	upload_chkfile_sh='hadoop fs -put ./119.chk /user/hive/warehouse/asiainfoh.db/bdi_monitor/119chk'
	
	os.popen(check_119chk_sh).readlines()
	os.popen(check_120chk_sh).readlines()
	f_119=open('check_upload_119.txt').readlines()
	f_120=open('check_upload_120.txt').readlines()
	
	#print len(f)
	
	if len(f_119) == 0 :
		upload_flag=True
	
	#os.popen(check_119chk_sh).readlines()
	
	#f_check_upload=open('check_upload.txt')
	
	#f=f_check_upload.readlines()
	#f=open('check_upload.txt').readlines()
	
	#f_check_upload.close()
	
	if len(f_120) != 0:
		upload_flag=False
		# 计算hdfs校验文件并删除校验文件
		print "计算hdfs校验文件并删除校验文件"
		
		get_running_num()
		
	
	# 上传校验文件到hdfs
	if upload_flag==True:
		os.popen(upload_chkfile_sh).readlines()
		print "上传校验文件到hdfs"
	else:
		print "下次上传"
	
	
	#print type(f)
	#print 'f[0]',f[0]
	
	
	
	# 生成校验文件上传到hdfs
	
	# 查询solr将结果输出到search_solr.txt
	#solr_sh='curl -i -H "Content-Type:application/json" -X POST --data \''+'{"condition":"activity_num:'+self.activity_num+' AND start_time:{\\"'+self.start_time_stamp+'\\" TO \\"'+self.end_time_stamp+'\\"}" ,"tables":["DW_LOC_ZX_USER_BH_MIN_20190917"],"query":"","start":0,"return_fields":["phone","imsi","start_time","city_id","county_id"],"cursor_mark":"*","sort":"id desc","rows":10000}\' '+'http://10.218.146.65:9000/ocsearch-service/query/search | more'+' > '+config.log_path+'solr_monitor.txt'
	
	#print solr_sh
	
	#os.popen(solr_sh).readlines()

# 将hdfs上的校验文件下载到本地，进行计算
def get_running_num():
	
	get_119chk_sh='hadoop fs -get /user/hive/warehouse/asiainfoh.db/bdi_monitor/119chk/* ./'
	get_120chk_sh='hadoop fs -get /user/hive/warehouse/asiainfoh.db/bdi_monitor/120chk/* ./'
	
	os.popen(get_119chk_sh).readlines()
	os.popen(get_120chk_sh).readlines()
	
	rm_119chk_sh="hadoop fs -rm /user/hive/warehouse/asiainfoh.db/bdi_monitor/119chk/*"
	rm_120chk_sh="hadoop fs -rm /user/hive/warehouse/asiainfoh.db/bdi_monitor/120chk/*"
	
	os.popen(rm_119chk_sh).readlines()
	os.popen(rm_120chk_sh).readlines()
	
	f_119=open('119.chk').readlines()
	f_120=open('120.chk').readlines()
	
	
	print f_119
	print type(f_119)
	
	print f_120
	print type(f_120)
	
	running_num=int(f_119[0])+int(f_120[0])
	
	print 'result',running_num
	
	rm_localchk_sh='rm *.chk'
	
	os.popen(rm_localchk_sh).readlines()
	
	# 产生校验文件，用于短信发送
	
	
	# 将短信内容写入日志
	os.popen('echo '+time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))+' '+str(running_num)+' >> '+'running_num.log').readlines()
	
	if running_num > 110:
		send_chk(running_num)
	

# 产生校验文件，用于短信发送
def send_chk(running_num):
	
	
	
	send_chk_sh='sh trans_data_bdi2sanqi_chk.sh 119chk 20200109 '+str(running_num)
	
	print '产生校验文件命令',send_chk_sh
	
	os.popen(send_chk_sh).readlines()
	
	print '产生校验文件，用于短信发送'

#create_running()
#create_chk()

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

