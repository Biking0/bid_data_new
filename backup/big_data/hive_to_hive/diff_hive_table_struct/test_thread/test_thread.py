#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：test_thread.py
# 功能描述：hive表数据稽核
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20200624
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python test_thread.py
# ***************************************************************************

import os
import sys
import threading
import time
from Queue import Queue


def sing(num, a):
    # print 'a', a
    # for i in range(num):
    #     print "sing%d" % i
    #     time.sleep(0.5)

    # for i in a:

    for i in range(a.qsize()):
        if not a.empty():
            print 'sing', a.get()
            time.sleep(1)


def dance(num, a):
    # print 'a', a
    # for i in range(num):
    #     print "sing%d" % i
    #     time.sleep(0.5)

    # for i in a:

    for i in range(a.qsize()):
        if not a.empty():
            print 'dance', a.get()
            insert_mysql()

            time.sleep(1)


def insert_mysql():
    insert_sql_sh = "mysql -h 172.19.168.22 -P 3308 -u zhao -pzhao zhao -e 'insert into test_thread (name) values('123')'"
    insert_str = os.popen(insert_sql_sh).readlines()
    print 'insert_str', insert_str


def main():
    """创建启动线程"""
    a = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    b = ['123', '456', '456', '5678', '123', '456', '456', '5678', '123', '456', '456', '5678', ]

    test_queue = Queue()

    for i in range(200):
        test_queue.put(i)

    for i in range(200):
        t_dance = threading.Thread(target=dance, args=(6, test_queue))
        t_dance.start()
    # t_sing = threading.Thread(target=sing, args=(5, a[0:3]))
    # t_sing1 = threading.Thread(target=sing, args=(5, test_queue))
    # t_sing2 = threading.Thread(target=sing, args=(5, b[3:5]))
    # t_sing = threading.Thread(target=sing, args=(5, b))
    # t_dance = threading.Thread(target=dance, args=(6, test_queue))
    # t_dance = threading.Thread(target=sing, args=(6, a[3:9]))
    # t_sing1.start()
    # t_sing2.start()
    # t_dance.start()


if __name__ == '__main__':
    main()
