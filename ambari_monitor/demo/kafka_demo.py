##!/usr/bin/env python
## -*-coding:utf-8 -*-
##********************************************************************************
## **  文件名称：kafka_demo.py
## **  功能描述：kafka服务监控demo
## **  输 入 表： 
## **  输 出 表： 
## **  创 建 者：hyn
## **  创建日期：20191010
## **  修改日志：
## **  修改日期：
## *******************************************************************************
## **  程序调用格式：python kafka_demo.py
## *******************************************************************************

import os
import json

# 是否发送短信，发送短信需要置为True
send_flag=True

# 请求kafka状态数据输出到kafka.txt
kafka_sh='curl -u admin:admin http://10.218.146.65:8080/api/v1/clusters/hbasecluster/services/KAFKA > kafka.txt'
result=os.popen(kafka_sh).readlines()

# 筛选条件
all_info='ALL'

# 读取kafka本地文件信息
f=open('kafka.txt')
dict_data=json.load(f)

# kafka集群信息
kafka_info=dict_data['alerts_summary']
critical_info=kafka_info['CRITICAL']
host_info=kafka_info['OK']

# 短信内容
sms_list=[]

# kafka信息监控
if critical_info>-1:
	critical_sms_info='kafka 出现'+str(critical_info)+'条警告信息CRITICAL！'
	sms_list.append(critical_sms_info)
	print 'kafka 出现'+str(critical_info)+'条警告信息CRITICAL！'
	#os.popen('sh send_sms.sh'+' '+sms_info+' '+all_info).readlines()

if host_info<8:
	host_sms_info='kafka 正常主机数：'+str(host_info)
	sms_list.append(host_sms_info)
	print 'kafka 正常主机数：'+str(host_info)
	#os.popen('sh send_sms.sh'+' '+sms_info+' '+all_info).readlines()

# 发送短信
if send_flag:
	for sms_info in sms_list:
		#os.popen('sh send_sms.sh'+' '+sms_info+' '+all_info).readlines()
		print 'kafka 短信：'+sms_info


