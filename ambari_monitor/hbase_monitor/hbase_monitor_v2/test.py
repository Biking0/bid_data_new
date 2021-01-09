#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：hbase_monitor.py
# 功能描述：clean 180 days ago log
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20201231
# 修改日志：
# 修改日期：
# 位置：101
# ***************************************************************************
# 程序调用格式：python hbase_monitor.py
# ***************************************************************************


import json
import requests
import send_sms

user = 'admin'
password = 'admin'


# 构造url
def hbase_monitor():
    hostname_list = ['hua-dlzx2-a0207',
                     'hua-dlzx2-a0208',
                     'hua-dlzx2-a0209',
                     'hua-dlzx2-a0210',
                     'hua-dlzx2-b0207',
                     'hua-dlzx2-b0208',
                     'hua-dlzx2-b0209',
                     'hua-dlzx2-b0210',
                     'hua-dlzx2-c1210',
                     'hua-dlzx2-c1211',
                     'hua-dlzx2-c1212',
                     'hua-dlzx2-c1301',
                     'hua-dlzx2-c1302',
                     'hua-dlzx2-c1303',
                     'hua-dlzx2-c1304',
                     'hua-dlzx2-c1305',
                     'hua-dlzx2-c1306',
                     'hua-dlzx2-c1307',
                     'hua-dlzx2-c1308',
                     'hua-dlzx2-c1309',
                     'hua-dlzx2-c1310',
                     'hua-dlzx2-c1311',
                     'hua-dlzx2-c1312']

    error_rs = []

    for hostname in hostname_list:
        url = "http://172.19.168.110:8080/api/v1/clusters/hbasecluster/hosts/%s/host_components/HBASE_REGIONSERVER" % (
            hostname)

        result = requests.get(url, auth=(user, password))
        result_json = json.loads(result.text)

        # 节点停掉
        rs_status = result_json['HostRoles']['desired_state']

        if not rs_status == 'STARTED':
            error_rs.append(hostname)
            continue

        # 节点异常
        try:
            metrics_result = result_json['metrics']
        except Exception as e:

            print '节点异常', e
            error_rs.append(hostname)
            continue

    if len(error_rs) > 0:
        sms_info = "Hbase集群regionserver节点告警，主机：%s" % (str(error_rs)).replace('\'', '')
        print 'sms_info:', sms_info
        send_sms.send_sms(sms_info)
    else:
        print 'Hbase集群regionserver服务正常'


hbase_monitor()
