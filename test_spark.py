#!/usr/bin/env python
# -*-coding:utf-8 -*-
#********************************************************************************************
# **
# **  文件名称： dw_user_after_before_yyyymmdd.py
# **
# **  功能描述： 沉淀前后三天和前后一天的省内位置数据
# **
# **  输入参数：
# **
# **  输 入 表：    
# **
# **  输 出 表： dw_user_after_before_yyyymmdd
# **  创 建 者： hhy
# **
# **  创建日期： 2019年02月21日
# **
# **  修改日志：
# **
# **  修改日期：
# **
# **  修改人  ：
# **
# **  修改内容：
# **
# *******************************************************************************************
# **
# **  程序调用格式：python dw_user_after_before_yyyymmdd.py 20181006
# **
# *******************************************************************************************

import os,sys
import time,datetime
from settings import *
from hqltools3 import *
#from hqltools3_nt import *

#===========================================================================================
#程序开始 获得输入参数
#===========================================================================================

name = 'dw_user_after_before_yyyymmdd'
dates= '20191015'
#taskPosition=""

#创建1
hivesql = []
hivesql.append('''select * from dw_user_after_3day_yyyymmdd limit 5''')
HiveExe(hivesql,name,dates)