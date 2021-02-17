#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：pubUtil.py
# 功能描述：公共工具包
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20200706
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python pubUtil.py
# ***************************************************************************

import os
import sys
import time
import datetime


# 获取当天日期字符串
def get_today():
    today = datetime.date.today()

    today = str(today + datetime.timedelta(days=0)).replace('-', '')
    print today
    return today


# 获取当天前一天日期字符串
def get_pre_day():
    today = datetime.date.today()

    pre_day = str(today + datetime.timedelta(days=-1)).replace('-', '')
    print pre_day
    return pre_day


# 获取当月前一个月
def get_pre_month():
    today = datetime.date.today()
    first = today.replace(day=1)
    last_month = first - datetime.timedelta(days=1)
    pre_month = str(last_month.strftime("%Y%m"))

    print pre_month
    return pre_month


get_pre_day()
get_pre_month()
get_today()
