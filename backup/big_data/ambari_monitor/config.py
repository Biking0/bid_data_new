#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：config.py
# 功能描述：ambari监控项目配置文件
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20191010
# 修改日志：20191028
# 修改日期：
# ***************************************************************************
# 程序调用格式：python config.py
# ***************************************************************************

# hbase集群地址
hbase_url='http://10.218.146.65:8080/api/v1/clusters'

# hbase集群名字
hbase_name='hbasecluster'
hbase_name_CH='HBASE集群'

# 三期集群地址
sanqi_url='http://10.93.171.97:8080/api/v1/clusters'

# 三期集群名字
sanqi_name='hnydcluster'
sanqi_name_CH='三期集群'

# 监控目标地址
monitor_url=hbase_url
monitor_name=hbase_name

# 流处理相关服务
liu_service_list=['FLUME','KAFKA']

# 目前监控服务
# service_list=['AMBARI_INFRA','FLUME','HBASE','KAFKA','MAPREDUCE2','PIG','RANGER','SLIDER','SPARK','TEZ','YARN','ZOOKEEPER','HDFS']
# 停止监控flume，flume暂停几个agent
service_list=['AMBARI_INFRA','HBASE','KAFKA','ZOOKEEPER']

# 全部服务
all_service_list=['AMBARI_INFRA','AMBARI_METRICS','FLUME','HBASE','HDFS','HIVE','KAFKA','MAPREDUCE2','PIG','RANGER','SLIDER','SPARK','TEZ','YARN','ZOOKEEPER']

# 无法监控服务，ambari页面参数异常
error_service_list=['AMBARI_METRICS','HDFS','HIVE']

##### 短信参数配置 ###############################################################
# 短信过滤
all_info='ALL'

# 短信过滤测试
#all_info='HYN'

# 短信通知开关，是否发送短信，发送短信需要置为True，否则为False
send_flag=True

# 日志文件路径
log_path='log/'

# 日志名字
hbase_service_monitor='hbase_service_monitor'
sanqi_service_monitor='sanqi_service_monitor'

##### solr 参数配置 ##############################################################

# solr默认查询活动idactivity_num
# 10601 10061 10678 10221 10730 11586
default_activity_num = '10061'

# solr监控search表名
solr_table='DW_LOC_ZX_USER_BH_MIN_20200210'

# solr短信开关
solr_send_falg=True

# 查询最新30分钟
search_time=1800

# solr数据量警告值
solr_data_num=100

# 监控间隔
sleep_time=600

##### 程序监控名称，用于日志打印 ##################################################
service_monitor = 'service_monitor'
solr_monitor = 'solr_monitor'
kafka_monitor = 'kafka_monitor'

##### 程序监控开关，可停用指定监控程序 ############################################
service_monitor_flag = True
solr_monitor_flag = True
kafka_monitor_flag = True

###### 监控开关 #######
# 1. 监控各类服务
# 2. 监控solr
# 3. 
# 4. 监控kafka消费
# 5. 监控kafka日志，有监控