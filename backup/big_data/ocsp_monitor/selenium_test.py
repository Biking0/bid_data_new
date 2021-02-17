#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：selenium.py
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


import requests
import selenium

from selenium import webdriver as we
import time,json

url = 'http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=inst&opt_type=hour&jobFlowId=c88463b8-6c5c-4fab-b41c-685cbfb7db01&beginDate=&tasknum=-&name=DPS_DWD_ICA2_3&data_time_=20191020'
url_running="http://ly4-j10-04u-dl380-275:8088/cluster/apps/RUNNING"


# 获取session_id
def get_session_id():


	# url = 'https://wizzair.com/#/booking/select-flight/TIA/BUD/2018-12-22/null/1/0/0/0/null'
	get_cookies_url = 'https://wizzair.com/en-gb#/booking/select-flight/TIA/DTM/2018-12-20/null/1/0/0/0/null'
	
	url='http://10.93.171.80:8080/admin/schedule/mainmonitor_hn'
	url_running="http://ly4-j10-04u-dl380-275:8088/cluster/apps/RUNNING"
	driver = we.Chrome(executable_path='E:\soft\chromedriver.exe')
	
	driver.get(url_running)
	
	time.sleep(5)
	driver.find_element_by_xpath('//*[@id="login_username"]').send_keys('admin')
	driver.find_element_by_xpath('//*[@id="login_password"]').send_keys('11111')
	time.sleep(2)
	driver.find_element_by_xpath('//*[@id="loginIcon"]').click()
	
	
	cookies=driver.get_cookies()
	
	print (cookies)
	
	session_id=cookies[0]['value']
	
	print (session_id)
	
	time.sleep(5)
	
	return session_id
	driver.close()
	
	# driver=webdriver.Firefox(executable_path = 'D:\softInstall\Mozilla Firefox\geckodriver.exe')
	
	# driver=webbrowser.Firefox(executable_path = 'D:\softInstall\Mozilla Firefox\geckodriver.exe')
	

def monitor():
	
	#session_id=get_session_id()
	
	headers = {
    # 'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    # 'User-Agent': 'Dalvik/1.6.0 (Linux; U; Android 4.4.2; SM-G955N Build/NRD90M)',
    # 'Host': 'book.hop.com',
    # 'Connection': 'Keep-Alive',
    # 'Accept-Encoding': 'gzip',
    # 'Content-Length': '1845'
    'Cookie': 'JSESSIONID='+session_id}
	
	
	#result = requests.get(url, headers=headers)
	result = requests.get(url_running, headers=headers)
	
	print(result.text)
	
# 启动测试
if __name__=='__main__':

	monitor()






