#!/usr/bin/env python
# -*-coding:utf-8 -*-
#********************************************************************************
# ** 文件名称：migu_read.py
# ** 功能描述：咪咕阅读，需要手工运行
# ** 输 入 表：
# ** 输 出 表：
# ** 创 建 者：hyn
# ** 创建日期：20200220
# ** 修改日志：
# ** 修改日期：20200524
# *******************************************************************************
# ** 程序调用格式：nohup python migu_read.py 20200412 >> /home/ocetl/migu_read/log/migu_read.log &
# *******************************************************************************

# 执行流程
# 1.添加数据文件
# 2.由开发人员手动执行
# 3.加载数据，若数据不存在，提示错误
# 4.执行完成，备份数据文件，发送短信

import os
import sys
import time
import datetime

# 咪咕数据文件路径
data_path='/home/ocetl/migu_read/data'
# 咪咕数据文件备份路径
backup_path='/home/ocetl/migu_read/backup'
# 日志路径
log_path = '/home/ocetl/migu_read/log'

# 项目路径
project_path='/home/ocetl/migu_read'


# 数据文件检测
def file_check(input_date):
	data_count=os.listdir(data_path)
	
	print len(data_count)
	
	# 只能放一个文件
	if len(data_count) == 1:
		
		# 开始执行程序
		load_data(input_date)
	
	else :		
		print '运行异常！'
		print '1.数据存放目录：',data_path
		print '2.只能放一个txt文件'

# 加载数据
def load_data(input_date):
	
	# 加载数据
	load_data_sh="hive -e \"set hive.exec.compress.output=false;set hive.cli.print.header=false; load data local inpath '"+data_path+"/cmread_hotlist_"+input_date+".txt' overwrite into table dm_cmread_booklist; \""
	
	print load_data_sh
	os.popen(load_data_sh)
	
	# 修改表名
	rename_table_sh="hive -e 'set hive.exec.compress.output=false;set hive.cli.print.header=false; drop table kf_01_mass_hostlist_2020_week5;alter table dm_cmread_booklist rename to  kf_01_mass_hostlist_2020_week5; '"
	
	print rename_table_sh
	os.popen(rename_table_sh)
	
	migu_read(input_date)

def migu_read(input_date):
	
	# 获取当天日期，命名表名
    #today_str = time.strftime('%Y%m%d', time.localtime(time.time()))
	
	# 调用程序,python sh
    # python /home/ocetl/yfx/migureading_running.py 20200207
    # sh /home/ocetl/yfx/recommend/six.sh kf_01_mass_cmread_rec_2020_week5_1_six_20200207 20200207
    # sh /home/ocetl/yfx/recommend/ten.sh kf_01_mass_cmread_rec_2020_week5_1_20200207 20200207	
    
    python_sh = 'python '+project_path+'/migureading_running.py' + ' ' + input_date + ' > '+log_path+'/migureading_running_' + input_date + '.log'
    six_sh = 'sh '+project_path+'/six.sh kf_01_mass_cmread_rec_2020_week5_1_six_' + input_date + ' ' + input_date + ' > '+log_path+'/six_' + input_date + '.log'
    ten_sh = 'sh '+project_path+'/ten.sh kf_01_mass_cmread_rec_2020_week5_1_' + input_date + ' ' + input_date + ' > '+log_path+'/ten_' + input_date + '.log'
    
    print python_sh
    print six_sh
    print ten_sh
    
    # 测试
    # python1_sh = 'nohup python /home/ocetl/hyn/test/sleep.py > python1_sh.log'
    # sh1 = 'nohup sh /home/ocetl/hyn/test/sleep.sh > sh1.log'
    # sh2 = 'nohup sh /home/ocetl/hyn/test/sleep2.sh > sh2.log'
    # print python1_sh
    # os.popen(python1_sh)
    # print sh1
    # os.popen(sh1)
    # print sh2
    # os.popen(sh2)
    
    # 执行脚本    
    os.popen(python_sh)
    os.popen(six_sh)
    os.popen(ten_sh)
    
    data_num_sh='more /home/ocetl/yfx/recommend/cmread_recommend_20200522.txt | wc -l'
    
    # 获取数据条数
    data_num=os.popen(data_num_sh).readlines()[0].replace('\n','')
    
    # 调用短信发送
    send_email(data_num)

def send_email(data_num):
	
	sms_info='咪咕短信已执行完,数据条数:'+data_num
	send_sh='sh send_sms.sh '+sms_info+' '+'HYN'
	
	print send_sh
	
	os.popen(send_sh)
	


# 启动
if __name__=='__main__':
	
	# 处理输入日期
	input_date=''
	input_length = len(sys.argv)
	print 'input_str: ',len(sys.argv)
	
	if input_length==2:
		
		input_date=sys.argv[1]
		print '输入参数：',input_date
		
		file_check(input_date)
		
	else :
		print "参数输入有误，重新运行"