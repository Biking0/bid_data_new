#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：size_monitor.py
# 功能描述：添加统计信息
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20200119
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python size_monitor.py
# ***************************************************************************

import os
import sys
import datetime as date_time
import send_sms
import json

# 报错警戒值
error_value = 0.8


# 查询导出文件，读取控制台消息

# 监控空间跟文件数使用率
def size_monitor():
    now_time = str(date_time.datetime.now())[0:10].replace('-', '').replace(' ', '').replace(':', '')

    sql = "select * from tb_hdfs_size_used_result where statis_date='%s'" % (now_time)
    sh = "beeline -u 'jdbc:hive2://172.19.168.101:10000/csap' -n ocdp -p 1q2w1q@W --showHeader=false --outputformat=dsv --delimiterForDSV$'\t' -e \" %s \"" % (
        sql)

    print 'sh', sh

    result = os.popen(sh).readlines()

    # 结果表列表
    print 'result', result

    sms_info_list = []

    # 遍历租户使用信息
    for i in range(len(result)):

        error_list = []

        result_list = result[i].replace('\n', '').split('|')
        path = result_list[2]
        size_str = result_list[5]
        file_str = result_list[8]

        print '监控信息', size_str, file_str

        # 空间使用超过警戒值
        if size_str <> 'NULL' and float(size_str) > error_value:
            size_info = "空间使用警告：%s %s" % (path, size_str)
            error_list.append(size_info)

        # 文件数使用超过警戒值
        if file_str <> 'NULL' and float(file_str) > error_value:
            file_info = "文件数使用警告：%s %s" % (path, file_str)
            error_list.append(file_info)

        # 触发警告
        if len(error_list) > 0:
            print "触发警告:", error_list, str(error_list)
            sms_info_list.append(error_list)

    # 触发警告
    if len(sms_info_list) > 0:

        sms_info_str = ""
        for i in sms_info_list:
            team_str = ""
            for j in i:
                team_str = team_str + j

            sms_info_str = sms_info_str + ' ' + team_str

        sms_info = "集群资源空间文件数使用警告:" + sms_info_str.replace('\'', '')

        # print json.dumps(sms_info_list, encoding="UTF-8", ensure_ascii=False)
        print sms_info

        send_sms.send_sms(sms_info)


size_monitor()
