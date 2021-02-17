#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：sanqi_monitor.py
# 功能描述：监控当天的小时任务，每15分钟检测一次，出现报错任务短信通知
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20191023
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python sanqi_monitor.py
# ***************************************************************************

# 方法设计：
# 1.请求网络
# 2.短信内容编辑
# 3.发短信

import os
import sys
import time
import requests
import config


input_date='20191105'

class SanqiMonitor():
	
	# 初始化
	def __init__(self):
		
		# 筛选条件
		#self.all_info='ALL'
		# 短信内容
		self.sms_list=[]
		
		self.header = {
		'Cookie': 'JSESSIONID='+config.session_id}
		
		# 查询日期
		#self.input_date=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time))
		self.input_date=time.strftime('%Y%m%d',time.localtime(int(time.time())))
		print ('input time: ',self.input_date)
		
	# 网络请求
	def request_data(self):
		
		# 监控小时任务		
		self.hour_monitor()
		# 监控日任务
		self.day_monitor()
		
		
		

	def hour_monitor(self):
		
		# 1.解析小时任务
		for hour_url in config.hour_url_list:
			
			result = requests.get(hour_url+self.input_date, headers=self.header)
	
			#print(result.text)
			#print(type(result.text))
			
			result_data=result.text
			
			#print (list(result_data))
			
			result_list=[]
			
			# 解析任务个数
			for i in range(len(result_data)):
				if result_data[i]=='"':
				# print(i)
					for j in range(i+1,len(result_data)):
						if result_data[j]=='"':
							#print (j)
							#print (result_data[i+1:j])
							target_str=result_data[i+1:j]
							
							if target_str ==',' or target_str == '':
								break
							
							result_list.append(target_str)
							
							
							
							break
			
			print('#',result_list)
			
			self.hour_sms_info(result_list)
	
	
	def day_monitor(self):
	
	
		day_input_date=time.strftime('%Y%m%d',time.localtime(int(time.time())-86400))
		print ('day input time: ',day_input_date)
		
		
		#pass
		# 2.解析日任务
		for day_url in config.day_url_list:
			
			
			
			
			result = requests.get(day_url+day_input_date, headers=self.header)
	    
			#print(result.text)
			#print(type(result.text))
			
			result_data=result.text
			
			#print (list(result_data))
			
			result_list=[]
			
			# 解析任务个数
			for i in range(len(result_data)):
				if result_data[i]=='"':
				# print(i)
					for j in range(i+1,len(result_data)):
						if result_data[j]=='"':
							#print (j)
							#print (result_data[i+1:j])
							target_str=result_data[i+1:j]
							
							if target_str ==',' or target_str == '':
								break
							
							result_list.append(target_str)
							
							
							
							break
			
			print('#',result_list)
			
			self.day_sms_info(result_list)

			
	# 短信内容编辑
	def day_sms_info(self,result_list):
	
		day_id=result_list[0]
		day_name=config.day_dict.get(day_id)
		
		test='日任务错误数量'
		#test='error:'
		
		error_num=int(result_list[3])
		
		# 是否有错误任务
		if error_num> 0:
			self.sms_list.append(day_name+'：'+test+str(error_num))
		
		
		
		#print(self.sms_list)
		
		#
		# 发送短信
		if config.send_flag:
			self.send_sms()
			
	
	# 短信内容编辑
	def hour_sms_info(self,result_list):
	
		hour_id=result_list[0]
		hour_name=config.hour_dict.get(hour_id)
		# 获取任务个数
		for i in range(2,len(result_list)):
			
			
			#print(result_list[i])
			
			result_int=int(result_list[i].split('/')[1])
			
			
			#test=str('时段任务错误数量',encoding='gbk')
			#test=str('时段任务错误数量',encoding='utf-8')
			test='时段任务错误数量'
			#test='error:'
			
			if result_int> 0:
				#self.sms_list.append(hour_name+'：'+str(i-2)+test+str(result_int))
				#self.sms_list.append('短信测试！'+hour_name+'：'+str(i-2)+test+str(result_int))
				self.sms_list.append(hour_name+'：'+str(i-2)+test+str(result_int))
			
			
		
		
		#print(self.sms_list)
		
		#
		# 发送短信
		if config.send_flag:
			self.send_sms()
	
	# 发送短信
	def send_sms(self):
		#pass
		for sms_info in self.sms_list:
			
			# 发送短信
			os.popen('sh send_sms.sh'+' '+sms_info+' '+config.all_info+' >> '+config.log_path+'sanqi_monitor.log').readlines()
			#os.popen('sh send_sms.sh'+' '+sms_info+' '+config.hyn_info+' >> '+config.log_path+'sanqi_monitor.log').readlines()
			
			# 将短信内容写入日志
			os.popen('echo '+time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))+' '+sms_info+' >> '+config.log_path+'sanqi_monitor.log').readlines()
			
			print (' 短信：'+sms_info)
			#break
		
		self.sms_list=[]

## 启动测试
#if __name__=='__main__':
#	
#	sanqi_monitor_object = SanqiMonitor()
#	sanqi_monitor_object.request_data()
#	
#	#sm = ServiceMonitor()
#	#
#	#while True:
#	#	sm.request_data()
#	#	time.sleep(config.sleep_time)
#	
#	
