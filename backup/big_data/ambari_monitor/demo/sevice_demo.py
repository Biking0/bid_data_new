##!/usr/bin/env python
## -*-coding:utf-8 -*-
##********************************************************************************
## **  文件名称：service_demo.py
## **  功能描述：各类服务通用监控demo
## **  输 入 表： 
## **  输 出 表： 
## **  创 建 者：hyn
## **  创建日期：20191010
## **  修改日志：
## **  修改日期：
## *******************************************************************************
## **  程序调用格式：python service_demo.py KAFKA
## *******************************************************************************

import os
import sys
import json

# 是否发送短信，发送短信需要置为True
send_flag=True

# 默认为kafka，服务名称:KAFKA,FLUME,YARN,AMBARI_INFRA,HBASE
service_name=sys.argv[1]
if service_name is '':
	service_name='FLUME'
print '服务器名：'+service_name

# 请求kafka状态数据输出到kafka.txt
kafka_sh='curl -u admin:admin http://10.218.146.65:8080/api/v1/clusters/hbasecluster/services/'+service_name+' > '+service_name+'.txt'
result=os.popen(kafka_sh).readlines()

# 筛选条件
all_info='ALL'

# 读取kafka本地文件信息
f=open(service_name+'.txt')
dict_data=json.load(f)

# service集群信息
service_info=dict_data['alerts_summary']
critical_info=service_info['CRITICAL']
host_info=service_info['OK']
host_count=len(dict_data['alerts'])
print service_name+' 服务总主机数：'+str(host_count)

# 短信内容
sms_list=[]

# service信息监控
if critical_info>-1:
	critical_sms_info=service_name+' 出现'+str(critical_info)+'条警告信息CRITICAL！'
	sms_list.append(critical_sms_info)
	print critical_sms_info
	#os.popen('sh send_sms.sh'+' '+sms_info+' '+all_info).readlines()

if host_info<host_count+1:
	host_sms_info=service_name+' 正常主机数：'+str(host_info)
	sms_list.append(host_sms_info)
	print host_sms_info
	#os.popen('sh send_sms.sh'+' '+sms_info+' '+all_info).readlines()

# 发送短信
if send_flag:
	for sms_info in sms_list:
		# os.popen('sh send_sms.sh'+' '+sms_info+' '+all_info).readlines()
		print service_name+' 短信：'+sms_info


