#!/usr/bin/env python
# -*-coding:utf-8 -*-
# *******************************************************************************
# * 文件名称：search_solr_tool.py
# * 功能描述：solr查询工具，查询时间段内是否有数
# * 输 入 表：10041 20191015100000 20191015110000
# * 输 出 表： 
# * 创 建 者：hyn
# * 创建日期：20191015
# * 修改日志：不限制id
# * 修改日期：20200127
# *****************************************************************************
# * 程序调用格式：
# * 1.默认查询30分钟：                                                                          python search_solr_tool.py
# * 2.查询指定分钟时长（5分钟）:                                                        python search_solr_tool.py 5
# * 3.查询指定活动id，指定分钟时长                                                      python search_solr_tool.py 10678 5
# * 5.查询指定活动id，指定开始时间结束时间，时间精确到秒：      py search_solr_tool.py 10678 20191025220400 20191025223400
# *******************************************************************************

import os
import sys
import time
import json
import datetime
# import config

# 默认查询最新30分钟
search_time=30

# 默认activity_num
# 10601 10061 10678 10221 10730
default_activity_num = '10061'

# 查询solr
def search_solr(activity_num,start_time_stamp,end_time_stamp):

        # 查询solr将结果输出到search_solr.txt
        solr_sh='curl -i -H "Content-Type:application/json" -X POST --data \''+'{"condition":"activity_num:'+activity_num+' AND start_time:{\\"'+start_time_stamp+'\\" TO \\"'+end_time_stamp+'\\"}" ,"tables":["DW_LOC_ZX_USER_BH_MIN_20200210"],"query":"","start":0,"return_fields":["phone","imsi","start_time","city_id","county_id"],"cursor_mark":"*","sort":"id desc","rows":10000}\' '+'http://10.218.146.65:9000/ocsearch-service/query/search | more'+' > search_solr.txt'

        print solr_sh

        result=os.popen(solr_sh).readlines()

        # 读取本地文件信息
        f=open('search_solr.txt').readlines()

        dict_data=json.loads(f[6])

        # 打印查询结果，尽量不使用
        #print f

        print '限制活动id,solr最新'+str(search_time)+'分钟数据量：'+str(dict_data['data']['total'])

# 查询solr
def search_solr_allid(activity_num,start_time_stamp,end_time_stamp):

        # 查询solr将结果输出到search_solr.txt
        solr_sh='curl -i -H "Content-Type:application/json" -X POST --data \''+'{"condition":"start_time:{\\"'+start_time_stamp+'\\" TO \\"'+end_time_stamp+'\\"}" ,"tables":["DW_LOC_ZX_USER_BH_MIN_20200210"],"query":"","start":0,"return_fields":["phone","imsi","start_time","city_id","county_id"],"cursor_mark":"*","sort":"id desc","rows":10000}\' '+'http://10.218.146.65:9000/ocsearch-service/query/search | more'+' > search_solr.txt'

        print solr_sh

        result=os.popen(solr_sh).readlines()

        # 读取本地文件信息
        f=open('search_solr.txt').readlines()

        dict_data=json.loads(f[6])

        # 打印查询结果，尽量不使用
        #print f

        print 'solr最新'+str(search_time)+'分钟数据总量：'+str(dict_data['data']['total'])


# 启动
if __name__=='__main__':

        print '# 1.python search_solr_tool.py (默认查询30分钟)'
        print '# 2.python search_solr_tool.py 5 (查询指定分钟时长,5分钟）)'
        print '# 3.python search_solr_tool.py 10601 5 (查询指定活动id，指定分钟时长)'
        print '# 4.py search_solr_tool.py 10601 20191025220400 20191025223400 (查询指定活动id，指定开始时间结束时间，时间精确到秒)'

        input_str=sys.argv[0]
        input_length = len(sys.argv)
        print 'input_str: ',input_str,len(sys.argv)


        # 处理查询参数
        activity_num= default_activity_num
        # 转化为时间戳
        end_time_stamp = str(time.time()).split('.')[0]
        start_time_stamp = str(int(end_time_stamp)-search_time*60)

        # if len(sys.argv)

        # 1.默认查询30分钟
        if input_length == 1:

                pass

        # 2.查询指定分钟时长
        if input_length == 2:
                search_time=int(sys.argv[1])
                end_time_stamp = str(time.time()).split('.')[0]
                start_time_stamp = str(int(end_time_stamp)-search_time*60)


        # 3.查询指定活动id，指定分钟时长
        if input_length == 3:
                search_time=int(sys.argv[2])
                activity_num=sys.argv[1]
                end_time_stamp = str(time.time()).split('.')[0]
                start_time_stamp = str(int(end_time_stamp)-search_time*60)

        # 4.查询指定活动id，指定开始时间结束时间
        if input_length == 4:

                activity_num=sys.argv[1]
                start_time = sys.argv[2]
                end_time = sys.argv[3]

                # 不用加3个000
                #start_time_stamp=str(time.mktime(time.strptime(start_time,'%Y%m%d%H%M%S'))).split('.')[0]+'000'
                #end_time_stamp=str(time.mktime(time.strptime(end_time,'%Y%m%d%H%M%S'))).split('.')[0]+'000'
                start_time_stamp=str(time.mktime(time.strptime(start_time,'%Y%m%d%H%M%S'))).split('.')[0]
                end_time_stamp=str(time.mktime(time.strptime(end_time,'%Y%m%d%H%M%S'))).split('.')[0]

        print '查询参数: ',activity_num,start_time_stamp,end_time_stamp

        # 查询solr
        search_solr(activity_num,start_time_stamp,end_time_stamp)
        search_solr_allid(activity_num,start_time_stamp,end_time_stamp)


