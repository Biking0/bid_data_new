#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：check_current_time.py
# 功能描述：检测当前时间是否处于当天的6-22点的时间段
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20191025
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python code_head.py
# ***************************************************************************

f = open('./1.txt', 'r')
f1 = open('./2.txt', 'w+')

result = f.readlines()

for i in range(len(result)):
    table='alter table '+result[i].replace('\n','') + ' rename to ' + result[i].replace('\n','') +'_20201123;\n'
    f1.write(table)

    # if i > 100:
    #     break

f1.close()
