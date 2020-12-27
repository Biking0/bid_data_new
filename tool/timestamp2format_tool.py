#!/usr/bin/env python
# -*-coding:utf-8 -*-
# *******************************************************************************
# 文件名称：timestamp2format_tool.py
# 功能描述：时间戳与格式化时间相互转换工具
# 输 入 表：10041 20191015100000 20191015110000
# 输 出 表： 
# 创 建 者：hyn
# 创建日期：20191015
# 修改日志：相互转换
# 修改日期：20191104
# ***************************************************************************
# 程序调用格式：
# 1.输入时间戳转化为格式化时间：python timestamp2format_tool.py 1572920510000
# 2.输入格式化时间转化为时间戳（年月日时分）：python timestamp2format_tool.py 201910151730
# 3.没有参数默认输出当前时间：python timestamp2format_tool.py
# *******************************************************************************

import sys
import time

# 时间戳转化为格式化时间
def timestamp2format_tool(timestamp):

	# 不取毫秒
	timestamp = timestamp[0:-3]
	
	formattime= time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(int(timestamp)))
	print formattime
	return formattime
	
# 格式化时间转化为时间戳
def format2timestamp_tool(input_str):
	print str(time.mktime(time.strptime(input_str,'%Y%m%d%H%M'))).split('.')[0]+'000'
	
def get_localtime():
	local_timestamp=str(time.time()).split('.')[0]+'000'
	local_formattime=timestamp2format_tool(local_timestamp)
	print '当前时间戳：',local_timestamp
	print '当前时间：',local_formattime
	
# 启动
if __name__=='__main__':	
	
	print '# 1.python timestamp2format_tool.py 1572920510000 （时间戳转化为格式化时间）'
	print '# 2.python timestamp2format_tool.py 201910151730 （格式化时间转化为时间戳（年月日时分））'
	print '# 3.python timestamp2format_tool.py （没有参数默认输出当前时间）'
	
	input_str=sys.argv[0]
	input_length = len(sys.argv)
	
	# 没有参数默认输出当前时间
	if input_length == 1:
		get_localtime()
		exit()
	
	try :
		input_str=sys.argv[1]
		
		print 'input:',input_str[0:2]
	except:
		print 'input error !'
		exit()
	
	# 输入为时间戳
	if input_str[0:2]=='15':
		timestamp2format_tool(input_str)
		
	
	# 输入为格式化时间
	elif input_str[0:2]=='20':
		format2timestamp_tool(input_str)
	
	else :
		print 'input error !'
		exit()
		
		
		