#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：conn_db.py
# 功能描述：连接vertica
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20200830
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python conn_db.py
# ***************************************************************************

import vertica_python


# 根据表名选库
def conn_db():
    conn_info = {'host': '172.19.74.63',
                 'port': 5433,
                 'user': 'csapdwd',
                 'password': 'qaQA#3e4r1',
                 'database': 'csapdwd',
                 # 'backup_server_node': ['123.456.789.123', 'invalid.com', ('10.20.82.77', 6000)]
                 }
    conn = vertica_python.connect(**conn_info)

    return conn


def select(sql):
    conn = conn_db()
    cursor = conn.cursor()
    cursor.execute(sql)

    result = cursor.fetchall()
    cursor.close()
    print type(result)

    # print result
    return result

# 并发问题，集合结果存到vt
# def insert():
