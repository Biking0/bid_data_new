#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：send_sms.py
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
 
import pymysql

def send_sms(sms_info):
	
	# 打开数据库连接
	#db = pymysql.connect("localhost","testuser","test123","TESTDB" )
	db = pymysql.connect("10.97.192.180","ngtassuite","Aj7y32h!","ngtassuite" )
	
	# 使用 cursor() 方法创建一个游标对象 cursor
	cursor = db.cursor()
	# SQL 插入语句
	
	#sql = "INSERT INTO EMPLOYEE(FIRST_NAME, LAST_NAME, AGE, SEX, INCOME) VALUES ('%s', '%s',  %s,  '%s',  %s)" % \ ('Mac', 'Mohan', 20, 'M', 2000)
	sql = "INSERT INTO tb_sys_sms_send_cur(serv_number, send_date,text,opt_user,opt_date ) SELECT serv_number, now(), '%s', opt_user, now() FROM tb_sys_sms_phone WHERE opt_user = 'ocetl' and sms_id = '%s'" %  (sms_info,'HYN')
	try:
		# 执行sql语句
		cursor.execute(sql)
		# 提交到数据库执行
		db.commit()
	except:
		# 如果发生错误则回滚
		db.rollback()
		
		# 关闭数据库连接
		db.close()

# 启动测试
if __name__=='__main__':	
	
	sms_info='三期任务监控，短信测试，请忽略！'
	send_sms(sms_info)
#	
#	while True:
#		
#		sm = SolrMonitor()
#		
#		sm.request_data()
#		# 10分钟间隔
#		time.sleep(config.sleep_time)