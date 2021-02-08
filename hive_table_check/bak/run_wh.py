bdi@HACC-BH02-SERVICE118:~/hyn/hive_table_check> cat run_wh.py
# -*-coding:utf-8 -*-
#********************************************************************************
# �ļ����ƣ�run_mysql_monitor.py
# ������������Ϊ������
# �� �� ��
# �� �� ��
# �� �� �ߣ�hyn
# �������ڣ�20191020
# �޸���־��20191226
# �޸����ڣ�
# ****************************************************************************
# ������ø�ʽ��nohup python run_wh.py >> nohup.out &
# *******************************************************************************

import os
import sys
import time
import demo
#import config
#import bdi_monitor_task


# ����
if __name__=='__main__':


  #input_length = len(sys.argv)
  #print 'input_str: ',len(sys.argv)
  #
  ## 1.Ĭ�ϼ��HBASE��Ⱥ:1����HBASE��Ⱥ��2�������ڼ�Ⱥ
  #monitor_server=1
  #if input_length == 2 and sys.argv[1]=='2':
  #
  #    monitor_server=2

  while True:

       # 1.��ظ������
       #service_monitor_object = service_monitor.ServiceMonitor(monitor_server)
       #service_monitor_object.request_data()
       #
       ## 2.���solr
       ## ���ڼ�Ⱥ�����solr
       #if monitor_server==1:
       #   solr_monitor_object = solr_monitor.SolrMonitor()
       #   solr_monitor_object.request_data()
       #
       ## 3.���kafka����
       #
       ## 4.���kafka��־���м��
       #
       #print('sleep 900s')
       #time.sleep(config.sleep_time)

       #bdi_monitor_task.create_running()

       demo.demo()

       #run_sh='sh trans_data_bdi2sanqi.sh yiqing 20200109 yiqing'

       #os.popen(run_sh).readlines()

       # ��Ϣ10���ӣ�600

       print 'sleep 120s'
       time.sleep(120)
