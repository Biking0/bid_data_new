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

f = open('test.txt', 'r')
f1 = open('test1.txt', 'w+')

result = f.readlines()

for i in range(len(result)):
    print result[i][0:3]
    if result[i][0:3] == '/*!' or result[i][0:2] == '--' or result[i][0:4] == 'DROP':
        continue
    str_33 = result[i][0:32]
    print str_33
    try:
        if str_33 == ') ENGINE=InnoDB DEFAULT CHARSET=':
            comment = result[i].split(' ')
            print comment
            if len(comment) < 5:
                result[i]=');'
            else:
                result[i] = comment[0] + comment[4]
            # result[i] = ')'
    except Exception as e:
        print e
        print result[i]
        break

    f1.write(result[i])

    # if i > 100:
    #     break

f1.close()
