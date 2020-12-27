#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ********************************************************************************
# 文件名称：get_cluter_info.py
# 功能描述：获取集群存储信息
# 输 入 表：
# 输 出 表：
# 创 建 者：
# 创建日期：
# 修改日志：
# 修改日期：
# *******************************************************************************
# 程序调用格式：python get_cluter_info.py
# *******************************************************************************

# curl -u admin:admin -H "X-Requested-By: ambari" "http://172.19.168.100:8080/api/v1/clusters/csap/services"

import os
import json
import requests

user = 'admin'
password = 'admin'
url = "http://172.19.168.100:8080/api/v1/clusters/csap/services/HDFS/components/NAMENODE"

result = requests.get(url, auth=(user, password))

result_json = json.loads(result.text)

# 空间已使用百分比
used = str(result_json['ServiceComponentInfo']['PercentRemaining'])

result_txt = open('./used.txt', 'w')
result_txt.write(used)
result_txt.close()

print '集群空间使用百分比：', used
print result.status_code
