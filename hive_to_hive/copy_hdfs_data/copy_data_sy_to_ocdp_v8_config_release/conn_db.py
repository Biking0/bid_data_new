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
    conn = pymysql.connect(host="172.19.168.22", port=3308, user="zhao", passwd="zhao", db="zhao", charset="utf8")

    return conn


# 查询数据
def select(sql):
    try:
        conn = conn_db()
        cursor = conn.cursor()

        cursor.execute(sql)

        result = cursor.fetchall()
        cursor.close()
        print type(result)

        # print result
        return result
    except Exception as e:
        print '#数据库查询异常'
        print e


# 插入及更新数据
def insert(sql):
    try:
        conn = conn_db()
        cursor = conn.cursor()

        cursor.execute(sql)

        result = cursor.fetchall()

        conn.commit()
        cursor.close()
        conn.close()

        print type(result)

        # print result
        return result
    except Exception as e:
        print '# 数据库插入异常'
        print e


# select("show tables;")

insert("insert into test (id) values ('123')")
