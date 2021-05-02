#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：main.py
# 功能描述：主方法，调用各个稽核
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20210425
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python main.py
# ***************************************************************************

import os
import sys
import hive_data_check
import hive_chk_data_check
import vt_data_check
from datetime import datetime
import datetime as date_time
import threading


# 主入口方法
# 多线程调用各个稽核方法

class Main():
    def __init__(self):
        pass

    # 主方法
    def main(self):
        # 创建实例
        hive = hive_data_check.Hive_data_check()
        hive_chk = hive_chk_data_check.Hive_data_check()
        vt = vt_data_check.Vt_data_check()

        # # 多线程初始化
        # hive_thread = threading.Thread(target=hive.read_table_name)
        # hive_chk_thread = threading.Thread(target=hive_chk.read_table_name)
        # # vt_thread = threading.Thread(target=vt.read_table_name())

        # 启动线程
        # hive_thread.start()
        # hive_chk_thread.start()
        # # vt_thread.start()

        hive.read_table_name()
        hive_chk.read_table_name()
        vt.read_table_name()

        print '####################'

    # 汇总结果
    def out_result(self):

        




test = Main()
test.main()
