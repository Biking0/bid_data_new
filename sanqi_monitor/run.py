#!/usr/bin/env python
# -*-coding:utf-8 -*-
#********************************************************************************
# ** �ļ����ƣ�run_mysql_monitor.py
# ** �������������ڵ��ȼ����Ŀ���������м�س���
# ** �� �� ��
# ** �� �� ��
# ** �� �� �ߣ�hyn
# ** �������ڣ�20191105
# ** �޸���־��
# ** �޸����ڣ�
# *******************************************************************************
# ** ������ø�ʽ��nohup python run_mysql_monitor.py >> nohup.out &
# *******************************************************************************

import os
import time
import config
import sanqi_monitor
#import solr_monitor

# ����
if __name__=='__main__':
	
	while True:
	
		# 1.���ڵ��ȼ��
		sanqi_monitor_object = sanqi_monitor.SanqiMonitor()
		sanqi_monitor_object.request_data()
		
		# 2.���solr
		#solr_monitor_object = solr_monitor.SolrMonitor()
		#solr_monitor_object.request_data()
		
		# 3.���kafka����
		
		# 4.���kafka��־���м��
		
		print('sleep 900s')
		time.sleep(config.sleep_time)
		
		#time.sleep(3)

