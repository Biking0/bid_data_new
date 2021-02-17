#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：bdi_monitor_task.py
# 功能描述：监控大数据平台任务积压情况
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20191229
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python code_head.py
# ***************************************************************************

# 导入baseModule模块，然后定义子集的class，必须继承BaseObject，
# 然后定义一个函数，把业务逻辑写入函数内，在函数头上加上装饰器，则自动实现通用逻辑的调用
# 业务逻辑如果太长则可以定义多个函数，然后在主函数（就是加装饰器的函数）内按子集的业务逻辑依次调用

import os
import sys

f=open('runing.txt').readlines()

print type(f)
a=0
for i in f:
        if '.log' in i:
                pass
        else :
                a=a+1
                print i
print f[0]
print 123

print 'a',a

#chk_file=open('119.chk','ab')
chk_file=open('119.chk','w')
chk_file.write(str(a))