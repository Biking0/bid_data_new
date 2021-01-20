#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：conn_db.py
# 功能描述：迁移Hive表
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20200808
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python conn_db.py
# ***************************************************************************

import os
import sys
from datetime import datetime
import datetime as date_time
import pymysql

mysql_sh = "mysql -h 172.19.168.22 -P 3308 -u zhao -pzhao zhao -e ' "


# 连接
def conn_db():
    conn = pymysql.connect(host="192.168.195.233", port=20031, user="csapdmcfg", passwd="iEXIMt3w!TFL9vkO", db="csapdmcfg", charset="utf8")

    return conn


# 查询数据
def select(sql):
    conn = conn_db()
    cursor = conn.cursor()

    cursor.execute(sql)

    result = cursor.fetchall()
    cursor.close()
    conn.close()
    # print result
    return result


# 插入及更新数据
def insert(sql):
    conn = conn_db()
    cursor = conn.cursor()

    cursor.execute(sql)

    result = cursor.fetchall()

    conn.commit()
    cursor.close()
    conn.close()

    # print type(result)

    # print result
    return result


# 批量插入及更新数据
def insert_batch(sql_list):
    conn = conn_db()
    cursor = conn.cursor()

    for sql in sql_list:
        cursor.execute(sql)

    conn.commit()
    cursor.close()
    conn.close()

    # result = cursor.fetchall()
    # print type(result)
    # print result

    return
