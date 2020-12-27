#!/usr/bin/env python
# -*-coding:utf-8 -*-
#********************************************************************************
# ** 文件名称：solr_monitor.py
# ** 功能描述：solr监控，查询最30分钟数据量
# ** 输入参数：
# ** 输 出 表：
# ** 创 建 者：hyn
# ** 创建日期：20191015
# ** 修改日志：
# ** 修改日期：
# *******************************************************************************
# ** 程序调用格式：python solr_monitor.py
# *******************************************************************************

import os
import sys
import time
import json
import datetime
import config

# 方法结构设计:
# 1.初始化
# 2.网络请求
# 3.短信内容编辑
# 4.发送短信

# solr监控
class SolrMonitor():

        # 初始化
        def __init__(self):

                # 短信内容
                self.sms_list=[]

                # 初始化查询参数
                end_time_stamp = str(time.time()).split('.')[0]
                self.activity_num=config.default_activity_num
                self.start_time_stamp=str(int(end_time_stamp)-config.search_time)+'000'
                self.end_time_stamp=end_time_stamp+'000'

        # 网络请求
        def request_data(self):

                # 查询solr将结果输出到search_solr.txt
                solr_sh='curl -i -H "Content-Type:application/json" -X POST --data \''+'{"condition":"activity_num:'+self.activity_num+' AND start_time:{\\"'+self.start_time_stamp+'\\" TO \\"'+self.end_time_stamp+'\\"}" ,"tables":["'+config.solr_table+'"],"query":"","start":0,"return_fields":["phone","imsi","start_time","city_id","county_id"],"cursor_mark":"*","sort":"id desc","rows":10000}\' '+'http://10.218.146.65:9000/ocsearch-service/query/search | more'+' > '+config.log_path+'solr_monitor.txt'

                print solr_sh

                os.popen(solr_sh).readlines()
                self.sms_info()


        # 短信内容编辑
        def sms_info(self):

                # 读取本地文件信息
                f=open(config.log_path+'solr_monitor.txt').readlines()

                dict_data=json.loads(f[6])

                total_num=dict_data['data']['total']

                # 测试短信发送
                #if True:
                # 数据量小于警戒值，出现异常
                if total_num<config.solr_data_num:

                        # 短信内容
                        solr_info='solr数据量异常，最新30分钟数据量：'+str(total_num)
                        #solr_info='测试!!!solr数据量异常，最新30分钟数据量：'+str(total_num)

                        self.sms_list.append(solr_info)
                        print solr_info

                        # 发送短信
                        if config.solr_send_falg:
                                self.send_sms()
                else:
                        print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))+' '+'solr正常,最新30分钟数据量：'+str(total_num)

        # 发送短信
        def send_sms(self):

                for sms_info in self.sms_list:

                        # 发送短信
                        os.popen('sh send_sms.sh'+' '+sms_info+' '+config.all_info+' >> '+config.log_path+'solr_monitor.log').readlines()

                        # 将短信内容写入日志
                        os.popen('echo '+time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))+' '+sms_info+' >> '+config.log_path+'solr_monitor.log').readlines()

                self.sms_list=[]


# 启动测试
#if __name__=='__main__':
#
#       while True:
#
#               sm = SolrMonitor()
#
#               sm.request_data()
#               # 10分钟间隔
#               time.sleep(config.sleep_time)
