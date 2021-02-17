#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：get_dataos_database.py
# 功能描述：获取sy数据源信息
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20200922
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python get_dataos_database.py
# ***************************************************************************

import os
import sys
import conn_dataos_db
import json
import datetime as date_time
import conn_local_db


# 获取数据源信息，获取列表类型数据
def get_info():
    get_info_sql = "select ds_name as name ,ds_acct as username,md5(ds_auth) as password,ds_conf from  dacp_meta_datasource "

    result_list = conn_dataos_db.select(get_info_sql)

    data_list = []
    # 将元组转为list
    for i in result_list:
        data_list.append(list(i))

    parser_json(data_list)


# 解析json数据
def parser_json(data_list):
    insert_list = data_list

    for i in insert_list:
        # print i

        # url参数转为json数据
        json_data = json.loads(i[3])

        url = ''
        # 解析url参数
        if json_data['dsType'] == 'sftp':
            # print i
            url = json_data['dsInstLoc']
            i[3] = url

            i.remove(i[3])
            i.append(url)

        elif json_data['dsType'] == 'mysql' or json_data['dsType'] == 'oracle':
            # print i
            url = json_data['url']
            i[3] = url

            i.remove(i[3])
            i.append(url)

            # 测试
            # break


        # 其他类型数据源移除
        else:

            end_time = str(date_time.datetime.now())[0:19]
            f = open('./log/database_check.log', 'a+')
            f.write(end_time + ': 其他类型数据源： ' + str(i)+'\n')
            f.close()

            insert_list.remove(i)

            # break

        # 测试
        # break

    insert_info(insert_list)


# 插入本地库
def insert_info(insert_list):
    end_time = str(date_time.datetime.now())[0:10].replace('-', '')

    for i in insert_list:
        print i

        insert_sql = "insert into ocdp_etl_metadata (statis_date,name,username,password,url)  values('%s','%s','%s','%s','%s') " % (
            end_time, i[0], i[1], i[2], i[3])

        conn_local_db.insert(insert_sql)

        # break


get_info()
