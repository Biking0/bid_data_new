#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：config.py
# 功能描述：三期监控配置文件
# 输 入 表：
# 输 出 表：
# 创 建 者：
# 创建日期：
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python config.py
# ***************************************************************************

# 重要，程序运行失败，需要重新获取session_id
session_id='AB91B32FCDD3F904BC0B988167238F46'

# 监控间隔,15分钟
sleep_time=900

# 短信过滤
all_info='ALL'

# 短信测试
hyn_info='HYN'

# 短信通知开关，是否发送短信，发送短信需要置为True，否则为False
send_flag=True

# 日志文件路径
log_path='log/'

# 小时任务，先配置监控小时任务，session 15分钟内请求不会失效
hour_2='http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=init&jobFlowId=e8a7a6d7-5743-472e-9278-060b51e5c45b&name=DPS_CHECK_TIME'
hours_2='http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=inst&opt_type=hour&jobFlowId=e8a7a6d7-5743-472e-9278-060b51e5c45b&beginDate=&tasknum=-&name=DPS_CHECK_TIME&data_time_=20191022'

hour_23='http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=init&jobFlowId=c88463b8-6c5c-4fab-b41c-685cbfb7db01&name=DPS_DWD_ICA2_3'
hours_23='http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=inst&opt_type=hour&jobFlowId=c88463b8-6c5c-4fab-b41c-685cbfb7db01&beginDate=&tasknum=-&name=DPS_DWD_ICA2_3&data_time_=20191022'

hour_11='http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=init&jobFlowId=08725857-6056-4ad5-8414-db64d4704ac7&name=DPS_DW_ICA2_3'
hours_11='http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=inst&opt_type=hour&jobFlowId=08725857-6056-4ad5-8414-db64d4704ac7&beginDate=&tasknum=-&name=DPS_DW_ICA2_3&data_time_=20191022'

hour_26='http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=init&jobFlowId=34dc49ed-afe5-4302-9b7c-29ee336ce36f&name=DPS_ODS_LTE'
hours_26='http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=inst&opt_type=hour&jobFlowId=34dc49ed-afe5-4302-9b7c-29ee336ce36f&beginDate=&tasknum=-&name=DPS_ODS_LTE&data_time_=20191022'

hour_24='http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=init&jobFlowId=34dc49ed-afe5-4302-9b7c-29ee336ce36f&name=DPS_ODS_LTE'
hours_24='http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=inst&opt_type=hour&jobFlowId=143f4fa7-7bf5-4cd0-a7ba-5398b9256598&beginDate=&tasknum=-&name=DPS_ODS_TIME&data_time_=20191104'

# 小时任务列表
hour_url_list=['http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=inst&opt_type=hour&jobFlowId=e8a7a6d7-5743-472e-9278-060b51e5c45b&beginDate=&tasknum=-&name=DPS_CHECK_TIME&data_time_=','http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=inst&opt_type=hour&jobFlowId=c88463b8-6c5c-4fab-b41c-685cbfb7db01&beginDate=&tasknum=-&name=DPS_DWD_ICA2_3&data_time_=','http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=inst&opt_type=hour&jobFlowId=08725857-6056-4ad5-8414-db64d4704ac7&beginDate=&tasknum=-&name=DPS_DW_ICA2_3&data_time_=','http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=inst&opt_type=hour&jobFlowId=34dc49ed-afe5-4302-9b7c-29ee336ce36f&beginDate=&tasknum=-&name=DPS_ODS_LTE&data_time_=','http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=inst&opt_type=hour&jobFlowId=143f4fa7-7bf5-4cd0-a7ba-5398b9256598&beginDate=&tasknum=-&name=DPS_ODS_TIME&data_time_=']

# 日任务列表，无oracle迁移
day_url_list=[
'http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=inst&opt_type=day&jobFlowId=76694ea5-71f3-446d-bcc6-47600875f3eb&beginDate=&tasknum=-&name=A_DPS_DAY_TIME&data_time_=',
'http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=inst&opt_type=day&jobFlowId=dfb9ac39-6722-401b-83b2-c3a6548f3cc7&beginDate=&tasknum=-&name=A_DPS_DWD_DW_LIST_DAY&data_time_=',
'http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=inst&opt_type=day&jobFlowId=799c9945-2c1d-4293-bc2c-632807ca312f&beginDate=&tasknum=-&name=DPS_CHECK_DAY&data_time_=',
'http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=inst&opt_type=day&jobFlowId=c2f40da1-441a-4fbe-a293-ac25c4f909b3&beginDate=&tasknum=-&name=DPS_DW_DAY_CONT&data_time_=',
'http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=inst&opt_type=day&jobFlowId=d5d17fdc-743a-42a2-864a-30c83a9de8d7&beginDate=&tasknum=-&name=DPS_DW_DAY_Q&data_time_=',
'http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=inst&opt_type=day&jobFlowId=b9522283-c2a0-4045-b25b-40a525ebdd92&beginDate=&tasknum=-&name=DPS_DW_DM_DAY&data_time_=',
'http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=inst&opt_type=day&jobFlowId=702e50d2-8558-4752-b35d-519d68b701e7&beginDate=&tasknum=26&name=DPS_LOAD_DAY_APP&data_time_=',
'http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=inst&opt_type=day&jobFlowId=8e4d7a31-1704-42c5-a359-cd56f04abf6a&beginDate=&tasknum=-&name=DPS_LOAD_DAY_CONT&data_time_=',
'http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=inst&opt_type=day&jobFlowId=5bdf3454-fc93-40c6-8941-9b54e4611007&beginDate=&tasknum=-&name=DPS_LOAD_DAY_SITE&data_time_=',
# 关闭维表及经分日任务监控
#'http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=inst&opt_type=day&jobFlowId=d3611ddb-97e2-45b3-92ec-86c26bdf31af&beginDate=&tasknum=-&name=DPS_ODS_DAY_DIM&data_time_=',
#'http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=inst&opt_type=day&jobFlowId=efbdd0df-f9b0-4e26-b42c-60fcd30512c0&beginDate=&tasknum=-&name=DPS_ODS_DAY_P1&data_time_=',
'http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=inst&opt_type=day&jobFlowId=6101174a-f1b9-4e20-9633-14ea8708abaa&beginDate=&tasknum=-&name=DPS_ST_DAY_APP&data_time_=',
'http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=inst&opt_type=day&jobFlowId=f8f7f4de-3c48-4515-885b-580250a324c2&beginDate=&tasknum=-&name=DPS_ST_DAY_BASE&data_time_=',
'http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=inst&opt_type=day&jobFlowId=00e21364-bd8a-4ba7-9f29-3dd542306913&beginDate=&tasknum=-&name=DPS_ST_DAY_CONT&data_time_=',

]

# 日任务
day_30='http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=init&jobFlowId=76694ea5-71f3-446d-bcc6-47600875f3eb&name=A_DPS_DAY_TIME'

days_30='http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=inst&opt_type=day&jobFlowId=76694ea5-71f3-446d-bcc6-47600875f3eb&beginDate=&tasknum=-&name=A_DPS_DAY_TIME&data_time_=20191022'


day_14='http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=init&jobFlowId=dfb9ac39-6722-401b-83b2-c3a6548f3cc7&name=A_DPS_DWD_DW_LIST_DAY'
days_14='http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=inst&opt_type=day&jobFlowId=dfb9ac39-6722-401b-83b2-c3a6548f3cc7&beginDate=&tasknum=-&name=A_DPS_DWD_DW_LIST_DAY&data_time_=20191022'

day_12='http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=init&jobFlowId=799c9945-2c1d-4293-bc2c-632807ca312f&name=DPS_CHECK_DAY'
days_12='http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=inst&opt_type=day&jobFlowId=799c9945-2c1d-4293-bc2c-632807ca312f&beginDate=&tasknum=-&name=DPS_CHECK_DAY&data_time_=20191022'



day_21='http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=init&jobFlowId=c2f40da1-441a-4fbe-a293-ac25c4f909b3&name=DPS_DW_DAY_CONT'
days_21='http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=inst&opt_type=day&jobFlowId=c2f40da1-441a-4fbe-a293-ac25c4f909b3&beginDate=&tasknum=-&name=DPS_DW_DAY_CONT&data_time_=20191022'


day_44=''
days_44='http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=inst&opt_type=day&jobFlowId=d5d17fdc-743a-42a2-864a-30c83a9de8d7&beginDate=&tasknum=-&name=DPS_DW_DAY_Q&data_time_=20191022'


day_26='http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=init&jobFlowId=b9522283-c2a0-4045-b25b-40a525ebdd92&name=DPS_DW_DM_DAY'
days_26='http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=inst&opt_type=day&jobFlowId=b9522283-c2a0-4045-b25b-40a525ebdd92&beginDate=&tasknum=-&name=DPS_DW_DM_DAY&data_time_=20191022'

day_22='http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=init&jobFlowId=8e4d7a31-1704-42c5-a359-cd56f04abf6a&name=DPS_LOAD_DAY_CONT'
days_22='http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=inst&opt_type=day&jobFlowId=8e4d7a31-1704-42c5-a359-cd56f04abf6a&beginDate=&tasknum=-&name=DPS_LOAD_DAY_CONT&data_time_=20191031'
day_2='http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=init&jobFlowId=5bdf3454-fc93-40c6-8941-9b54e4611007&name=DPS_LOAD_DAY_SITE'
days_2='http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=inst&opt_type=day&jobFlowId=5bdf3454-fc93-40c6-8941-9b54e4611007&beginDate=&tasknum=-&name=DPS_LOAD_DAY_SITE&data_time_=20191031'

day_41='http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=init&jobFlowId=d3611ddb-97e2-45b3-92ec-86c26bdf31af&name=DPS_ODS_DAY_DIM'
days_41='http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=inst&opt_type=day&jobFlowId=d3611ddb-97e2-45b3-92ec-86c26bdf31af&beginDate=&tasknum=-&name=DPS_ODS_DAY_DIM&data_time_=20191031'

day_39='http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=init&jobFlowId=efbdd0df-f9b0-4e26-b42c-60fcd30512c0&name=DPS_ODS_DAY_P1'
days_39='http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=inst&opt_type=day&jobFlowId=efbdd0df-f9b0-4e26-b42c-60fcd30512c0&beginDate=&tasknum=-&name=DPS_ODS_DAY_P1&data_time_=20191031'

day_23='http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=init&jobFlowId=6101174a-f1b9-4e20-9633-14ea8708abaa&name=DPS_ST_DAY_APP'
days_23='http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=inst&opt_type=day&jobFlowId=6101174a-f1b9-4e20-9633-14ea8708abaa&beginDate=&tasknum=-&name=DPS_ST_DAY_APP&data_time_=20191031'

day_20='http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=init&jobFlowId=f8f7f4de-3c48-4515-885b-580250a324c2&name=DPS_ST_DAY_BASE'
days_20='http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=inst&opt_type=day&jobFlowId=f8f7f4de-3c48-4515-885b-580250a324c2&beginDate=&tasknum=-&name=DPS_ST_DAY_BASE&data_time_=20191031'

day_10='http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=init&jobFlowId=00e21364-bd8a-4ba7-9f29-3dd542306913&name=DPS_ST_DAY_CONT'
days_10='http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=inst&opt_type=day&jobFlowId=00e21364-bd8a-4ba7-9f29-3dd542306913&beginDate=&tasknum=-&name=DPS_ST_DAY_CONT&data_time_=20191031'

day_27='http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=init&jobFlowId=8e255506-f846-450f-9d80-b786cef4ef0f&name=HIVE_ORACLE'
days_27='http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=inst&opt_type=day&jobFlowId=8e255506-f846-450f-9d80-b786cef4ef0f&beginDate=&tasknum=-&name=HIVE_ORACLE&data_time_=20191031'



hour_dict={'e8a7a6d7-5743-472e-9278-060b51e5c45b':'话单记录数据稽核任务【时】-2',
'c88463b8-6c5c-4fab-b41c-685cbfb7db01':'ICA2.3新模型DWD【时】-23',
'08725857-6056-4ad5-8414-db64d4704ac7':'ICA2.3新模型DW【时】-11',
'34dc49ed-afe5-4302-9b7c-29ee336ce36f':'lte口小时话单入库任务【时】-26',
'143f4fa7-7bf5-4cd0-a7ba-5398b9256598':'1.采集ODS数据【时】，触发2.ODS_DW_HOUR-24'
}

day_dict={
'76694ea5-71f3-446d-bcc6-47600875f3eb':'日任务组【日】',
'dfb9ac39-6722-401b-83b2-c3a6548f3cc7':'DPS_DWD_DW层LIST数据任务【日】',
'799c9945-2c1d-4293-bc2c-632807ca312f':'日任务稽核【日】',
'c2f40da1-441a-4fbe-a293-ac25c4f909b3':'DW层日数据计算【日】--内容',
'd5d17fdc-743a-42a2-864a-30c83a9de8d7':'DW层日数据计算【日】--需求[低]',
'b9522283-c2a0-4045-b25b-40a525ebdd92':'流量偏好标签【日】',
'702e50d2-8558-4752-b35d-519d68b701e7':'LOAD数据【日】--应用',
'8e4d7a31-1704-42c5-a359-cd56f04abf6a':'LOAD数据【日】--内容',
'5bdf3454-fc93-40c6-8941-9b54e4611007':'LOAD数据【日】--网站',
# 关闭维表及经分日任务监控
#'d3611ddb-97e2-45b3-92ec-86c26bdf31af':'维表数据同步【日】',
#'efbdd0df-f9b0-4e26-b42c-60fcd30512c0':'经分数据采集【日】',
'6101174a-f1b9-4e20-9633-14ea8708abaa':'ST层数据计算【日】--应用',
'f8f7f4de-3c48-4515-885b-580250a324c2':'ST层基础数据组【日】',
'00e21364-bd8a-4ba7-9f29-3dd542306913':'ST层数据计算【日】--内容',

}


