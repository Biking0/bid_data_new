#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：session_test.py
# 功能描述：三期任务监控
# 输 入 表：tb_dwd_mid_ci_loc_merge_hour      
# 输 出 表：tb_dwd_ci_loc_merge_hour
# 创 建 者：chengpx
# 创建日期：2017年11月10日
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python tb_dwd_ci_loc_merge_mc_hour.py 2015073001
# ***************************************************************************

# 测试session连续访问是否会过期
import requests
import time
import json

def request_data():
	url = 'http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=inst&opt_type=hour&jobFlowId=c88463b8-6c5c-4fab-b41c-685cbfb7db01&beginDate=&tasknum=-&name=DPS_DWD_ICA2_3&data_time_=20191020'


	headers = {
		# 'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
		# 'User-Agent': 'Dalvik/1.6.0 (Linux; U; Android 4.4.2; SM-G955N Build/NRD90M)',
		# 'Host': 'book.hop.com',
		# 'Connection': 'Keep-Alive',
		# 'Accept-Encoding': 'gzip',
		# 'Content-Length': '1845'
		'Cookie': 'JSESSIONID=4E6FD0DCFCBE14A06EF2E891B768FFA0'
	}
	
	result = requests.get(url, headers=headers)
	
	print(result.text)
	print(type(result.text))
	
	result_data=result.text
	
	# print (list(result_data))
	
	result_list=[]
	
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
				
				list_str.append(target_str)
				
				
				
				break
	
	print(result_list)
				
	
	
	
	
	
	
	f=open('log/session_test.log','a+')
	
	f.write(str(result.text))
	f.write(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())))
	
	f.close()

# 启动测试
if __name__=='__main__':
	while True:
		request_data()
		time.sleep(900)