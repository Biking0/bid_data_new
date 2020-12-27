#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：clean_zj_log.py
# 功能描述：clean 60 days ago log
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20200202
# 修改日志：清理solr日志
# 修改日期：20200329
# 位置：65:~/hyn
# ***************************************************************************
# 程序调用格式：python clean_zj_log.py
# ***************************************************************************

import os
import sys
import time
import shutil


path='/home/ocdp/hyn/clean_file/test'
file='/home/ocdp/hyn/clean_file/123.txt'

shutil.move(file,path)