#!/usr/bin/env python
# -*-coding:utf-8 -*-
#********************************************************************************
# �ļ����ƣ�run_mysql_monitor.py
# ����������ambari �����Ŀ�������м�س���
# �� �� ��
# �� �� ��
# �� �� �ߣ�hyn
# �������ڣ�20191020
# �޸���־��20191226
# �޸����ڣ�
# ****************************************************************************
# ������ø�ʽ��
# 1.Ĭ�ϼ��HBASE��Ⱥ��nohup python run_mysql_monitor.py >> nohup.out &
# 2.���HBASE��Ⱥ��nohup python run_mysql_monitor.py 1 >> nohup_hbase.out &
# 2.������ڼ�Ⱥ��nohup python run_mysql_monitor.py 2 >> nohup_sanqi.out &
# *******************************************************************************

import os
import sys
import time
import config
import service_monitor
import solr_monitor

# ����
if __name__=='__main__':
	
	
	input_length = len(sys.argv)
	print 'input_str: ',len(sys.argv)
	
	# 1.Ĭ�ϼ��HBASE��Ⱥ:1����HBASE��Ⱥ��2�������ڼ�Ⱥ
	monitor_server=1
	if input_length == 2 and sys.argv[1]=='2':
		
		monitor_server=2

	while True:
	
		# 1.��ظ������
		service_monitor_object = service_monitor.ServiceMonitor(monitor_server)
		service_monitor_object.request_data()
		
		# 2.���solr
		# ���ڼ�Ⱥ�����solr
		if monitor_server==1:
			solr_monitor_object = solr_monitor.SolrMonitor()
			solr_monitor_object.request_data()
		
		# 3.���kafka����
		
		# 4.���kafka��־���м��
		
		print('sleep 900s')
		time.sleep(config.sleep_time)
		
		#time.sleep(3)

