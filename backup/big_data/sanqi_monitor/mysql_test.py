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
 
import pymysql
 
# 打开数据库连接
#db = pymysql.connect("localhost","testuser","test123","TESTDB" )
db = pymysql.connect("10.97.192.180","ngtassuite","Aj7y32h!","ngtassuite" )
 
# 使用 cursor() 方法创建一个游标对象 cursor
cursor = db.cursor()
 
# 使用 execute()  方法执行 SQL 查询 
cursor.execute("SELECT VERSION()")
 
# 使用 fetchone() 方法获取单条数据.
data = cursor.fetchone()
 
print ("Database version : %s " % data)