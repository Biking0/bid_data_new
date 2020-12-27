#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：code_head.py
# 功能描述：python脚本头
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20191025
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python code_head.py
# ***************************************************************************

import os
import sys
import time

#hive -e "set hive.exec.compress.output=false;set hive.cli.print.header=false;select * from $table_name" >${local_data_path}cmread_recommend_$day_id.txt;

#hive_sh="hive -e "+"set hive.exec.compress.output=false;set hive.cli.print.header=false;select * from st_rs_fg_o_hn_yyyymmdd where day_id=20200131 "+">st_rs_fg_o_hn_yyyymmdd_20200131.txt;"
hive_sh="hive -e "+"select * from st_rs_fg_o_hn_yyyymmdd where day_id=20200131 "+">st_rs_fg_o_hn_yyyymmdd_20200131.txt;"
hive_sh="beeline -e "+"select * from st_rs_fg_o_hn_yyyymmdd where day_id=20200131 "+">st_rs_fg_o_hn_yyyymmdd_20200131.txt;"
print hive_sh
os.popen(hive_sh).readlines()

