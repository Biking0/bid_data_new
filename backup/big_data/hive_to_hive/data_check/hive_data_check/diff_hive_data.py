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

# 生产环境
# excute_desc_sh = "beeline -u 'jdbc:hive2://192.168.190.88:10000/csap' -n hive -p %Usbr7mx -e "

# 测试环境
excute_desc_sh = "beeline -u 'jdbc:hive2://172.22.248.19:10000/default' -n csap -p @WSX2wsx -e "


# 导出稽核结果表到文件
def export_chk_result(table_name):
    export_sql = 'select DES_TBL,CYCLICAL,COUNT1,SUM1,REMARK from chk_result;'

    export_sh = excute_desc_sh + ' \" ' + export_sql + ' \" ' + ' >> chk_result.txt'

    print 'export_sh', export_sh

    os.popen(export_sh).readlines()


def diff_data():
    pass
