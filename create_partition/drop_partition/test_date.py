#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：create_partition.py
# 功能描述：python程序不会自己建分区
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20200617
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python create_partition.py

# ***************************************************************************
import datetime
import time

import pandas as pd

print pd.bdate_range(end='202009',periods=5,freq='D')