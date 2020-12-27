#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：get_sanqi_picture.py
# 功能描述：获取三期空间截图，填写健康日报使用
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20200407
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python get_sanqi_picture.py
# ***************************************************************************

# 获取截图
# 

import selenium
from selenium import webdriver as we
import time,json,os

driver = we.Chrome(executable_path='E:\soft\chromedriver.exe')

url = "http://10.93.171.97:50070/dfshealth.html#tab-overview"

driver.get(url)

time.sleep(5)

picture_name=str(time.strftime('%Y%m%d%H%M%S',time.localtime()))

print(123)
print(picture_name)

js ="var q=document.documentElement.scrollTop=650"
driver.execute_script(js)

time.sleep(5)
driver.get_screenshot_as_file('E:\\sanqi_picture\\'+picture_name+'.png')
#driver.get_screenshot_as_file('E:\\sanqi_picture\\'+'123'+'.png')


driver.quit()


