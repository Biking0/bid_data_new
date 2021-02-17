#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：hive_data_check.py
# 功能描述：hive表数据稽核
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20200624
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python hive_data_check.py
# ***************************************************************************

import os
import sys
import time
import ocdp_hive_data_check
import diff_hive_data

# 启动
if __name__ == '__main__':

    while True:
        # 休息10分钟，600
        print 'sleep 600s'
        time.sleep(900)
