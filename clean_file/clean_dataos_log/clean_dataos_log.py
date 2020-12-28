#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：clean_dataos_log.py
# 功能描述：clean 180 days ago log
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20200202
# 修改日志：清理solr日志
# 修改日期：20200329
# 位置：65:~/hyn
# ***************************************************************************
# 程序调用格式：python clean_dataos_log.py
# ***************************************************************************


import os
import sys
import time
import shutil
import hashlib

# delete 2 day ago file
# day_180 = 180

# 临时测试
day_180 = 0

# 不通机器对应不同日志路径，不同备份路径
# 数据结构，{'主机1':[['源文件路径1','备份路径1'],['源文件路径2','备份路径2']],
#           '主机2':[['源文件路径2','备份路径2'],['源文件路径2','备份路径2']]}
log_path = {'172.19.168.83': [['/home/dacp/apps', '/data/dacp/apps'], ['/home/dacp/apps', '/data/dacp/apps']],
            '172.19.168.96': [
                ['/home/dacp/apps/dataflow-broker-3.5.0/logs', '/data/dataos_log/dataflow-broker-3.5.0/logs',
                 '/data/dataos_log/dataflow-broker-3.5.0/logs']],
            '172.22.248.18': [['/home/csap/hyn/clean_dataos_log/log', '/home/csap/hyn/clean_dataos_log/log/test']]
            }


# 获取当前本地机器ip
def get_ip():
    ip = os.popen('hostname -i').readline().replace('\n', '')
    print ip
    return ip


# 清理指定日期日志
def clean_log():
    delete_time = time.time() - 3600 * 24 * day_180
    ip = get_ip()

    # 数据格式，[['/home/csap/hyn/clean_dataos_log/log', '/home/csap/hyn/clean_dataos_log/log/test']]
    path_list = log_path.get(ip)

    # kafka日志不删，'/var/log/kafka'
    # '/home/ocdp/javaApp/zx_core_tomcat/logs','/data/javaAppLogs/zx_core','/home/ocdp/javaApp/zj_interface_apache-tomcat-7.0.73/logs','/data/javaAppLogs/zj_interface','/home/ocdp/javaApp/zj_task2_apache-tomcat-7.0.73/logs','/data/javaAppLogs/zx_task','/home/ocdp/javaApp/zx_web_tomcat/logs','/data/javaAppLogs/zj_web','/data']

    # 遍历需要清理文件路径
    for path in path_list:

        file_path = path[0]
        # backup_path = path[1]

        # 数据格式
        # ['/home/csap/hyn/clean_dataos_log/log', '/home/csap/hyn/clean_dataos_log/log/test']
        # path

        # 遍历路径下载文件
        for file in os.listdir(file_path):

            try:

                filename = file_path + os.sep + file

                # 路径加文件名
                # print  'filename', filename

                # 判断时间是否过期，文件创建时间。
                if os.path.getmtime(filename) < delete_time:

                    # print 'file:', file

                    # 关键文件不删
                    if file == 'template.json':
                        print 'template.json文件保留'
                        continue

                    # todo 如果是目录进行，进行遍历，只遍历一级子目录
                    # 如果是文件夹
                    if os.path.isdir(filename):

                        # 遍历路径下载文件
                        for child_file in os.listdir(filename):
                            child_file_name = filename + os.sep + child_file
                            # print 'child_file_name', child_file_name

                            # 判断时间是否过期，文件创建时间。
                            if os.path.getmtime(child_file_name) < delete_time:

                                # print 'child_file:', child_file

                                # 关键文件不删
                                if child_file == 'template.json':
                                    print 'template.json文件保留'
                                    continue

                                # 直接找文件，不找文件夹
                                if not os.path.isdir(child_file_name):

                                    # 过滤重要文件
                                    if 'jar' not in filename or 'xml' not in filename:
                                        #     # 删除文件
                                        # os.remove(child_file_name)

                                        print time.strftime('%Y-%m-%d %H:%M:%S',
                                                            time.localtime()), child_file_name + " is removed"



                    # 不是文件夹
                    else:

                        if 'jar' not in filename or 'xml' not in filename:
                            #     # 删除文件
                            # os.remove(filename)

                            print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()), filename + " is removed"

            except Exception as e:

                # 出现异常，继续循环
                print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()), e
                continue


# 备份日志，home日志备份到数据盘
def backup_log():
    delete_time = time.time() - 3600 * 24 * day_180
    ip = get_ip()

    # 数据格式，[['/home/csap/hyn/clean_dataos_log/log', '/home/csap/hyn/clean_dataos_log/log/test']]
    path_list = log_path.get(ip)

    # kafka日志不删，'/var/log/kafka'
    # '/home/ocdp/javaApp/zx_core_tomcat/logs','/data/javaAppLogs/zx_core','/home/ocdp/javaApp/zj_interface_apache-tomcat-7.0.73/logs','/data/javaAppLogs/zj_interface','/home/ocdp/javaApp/zj_task2_apache-tomcat-7.0.73/logs','/data/javaAppLogs/zx_task','/home/ocdp/javaApp/zx_web_tomcat/logs','/data/javaAppLogs/zj_web','/data']

    # 遍历需要清理文件路径
    for path in path_list:

        file_path = path[0]
        backup_path = path[1]

        # 数据格式
        # ['/home/csap/hyn/clean_dataos_log/log', '/home/csap/hyn/clean_dataos_log/log/test']
        # path

        # 遍历路径下载文件
        for file in os.listdir(file_path):

            try:

                filename = file_path + os.sep + file

                # 路径加文件名
                # print  'filename', filename

                # 判断时间是否过期，文件创建时间。
                if os.path.getmtime(filename) < delete_time:

                    # print 'file:', file

                    # 关键文件不删
                    if file == 'template.json':
                        print 'template.json文件保留'
                        continue

                    # todo 如果是目录进行，进行遍历，只遍历一级子目录
                    # 如果是文件夹
                    if os.path.isdir(filename):

                        # 遍历路径下载文件
                        for child_file in os.listdir(filename):
                            child_file_name = filename + os.sep + child_file
                            # print 'child_file_name', child_file_name

                            # 判断时间是否过期，文件创建时间。
                            if os.path.getmtime(child_file_name) < delete_time:

                                # print 'child_file:', child_file

                                # 关键文件不删
                                if child_file == 'template.json':
                                    print 'template.json文件保留'
                                    continue

                                # 直接找文件，不找文件夹
                                if not os.path.isdir(child_file_name):

                                    # 过滤重要文件
                                    if 'jar' not in filename or 'xml' not in filename:
                                        # 备份文件
                                        backup_file(child_file_name, backup_path)


                    # 不是文件夹
                    else:

                        if 'jar' not in filename or 'xml' not in filename:
                            # 备份文件
                            backup_file(filename, backup_path)

            except Exception as e:

                # 出现异常，继续循环
                print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()), e
                continue


# 备份文件
def backup_file(file_name, backup_path):
    # 备份文件，备份后文件名起别名
    shutil.move(file_name, backup_path + '/%s_%s' % (file_name, hashlib.sha256(str(time.time())).hexdigest()[0:5]))

    print time.strftime('%Y-%m-%d %H:%M:%S',
                        time.localtime()), file_name + " is removed"


clean_log()
# get_ip()
