#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：run_sms.py
# 功能描述：python脚本头
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20200407
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python run_sms.py
# ***************************************************************************

# 启动脚本，每个5分钟扫描文件夹
# 调用短信发送脚本
# 将已发送过的文件移到已发送文件夹bak

import os
import sys
import time
import shutil

# 待发送文件夹
send_sms_path='./send_sms'

# 已发送文件夹
bak_path='./bak'

def main():
    # 下载bdi信息		
    download_sh='sh download_bdi_info.sh test 20200414 test'
    sms_info=os.popen(download_sh).readlines()
    
    # 遍历路径下载文件
    for file in os.listdir(send_sms_path):
    		filename=send_sms_path+os.sep+file
    		
    		print '文件信息：',os.sep,file
    		# 判断时间是否过期
    		#if os.path.getmtime(filename)< delete_time:						
    				
    		# 删除文件
    		#os.remove(filename)
    		
    		print "发送短信路径",filename
    		
    		
    		
    		send_sms='sh send_sms.sh'
    		
    		# 发送用户过滤，给不同用户发短信		
    		
    		sms_info=os.popen(send_sms+' '+filename).readlines()
    		
    		# 文件移动追加时间，防止文件重复
    		file_time=time.strftime('%Y%m%d%H%M%S',time.localtime())
    		
    		# 将已发送过的文件移到已发送文件夹bak
    		shutil.move(filename,"./bak/"+file+'_'+file_time)
    		print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime()),filename+" is moved"



