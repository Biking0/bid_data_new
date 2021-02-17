#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：test_hostname.py
# 功能描述：判断当前是哪个主机
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20200721
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python test_hostname.py
# ***************************************************************************

import os
import sys
import time
import datetime

hostname_sh='hostname -i'

hostname_str=os.popen(hostname_sh).readline().replace('\n','')
print hostname_str