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
def conn_db(database, config):
    if config == '1':
        import config as config
    elif config == '2':

        import config_test as config

        print '测试库', config.test_database
    else:
        print '输入数据库参数有误'

    conn_info = {'host': config.vt_ip,
                 'port': 5433,
                 'user': database,
                 'password': config.test_database.get(database)[1],
                 'database': config.test_database.get(database)[0],
                 # 'backup_server_node': ['123.456.789.123', 'invalid.com', ('10.20.82.77', 6000)]
                 }
    conn = vertica_python.connect(**conn_info)

    return conn


def select(sql, database, config):
    conn = conn_db(database, config)
    cursor = conn.cursor()
    cursor.execute(sql)

    result = cursor.fetchall()
    cursor.close()
    conn.close()
    print type(result)

    # print result
    return result


# 并发问题，稽核结果存到vt
def insert(sql, database, config):
    conn = conn_db(database, config)
    cursor = conn.cursor()

    cursor.execute(sql)

    result = cursor.fetchall()

    conn.commit()
    cursor.close()
    conn.close()

    print type(result)

    # print result
    return result
