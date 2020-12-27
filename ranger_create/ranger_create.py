#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：ranger_create.py
# 功能描述：31省策略生成
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20201009
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python ranger_create.py
# ***************************************************************************

import os
import sys
import json
import time
import requests
import datetime as date_time
import conn_db

url = "http://172.19.168.231:6080/service/public/v2/api/policy"

headers = {
    'X-XSRF-HEADER': "valid",
    'Content-Type': "application/json",
    'Authorization': "Basic YWRtaW46MXEydyFRQFc=",
    'Cache-Control': "no-cache",
    'Postman-Token': "569270c0-c554-424a-9260-7cb22b3dfdd6"
}

# 变量：策略名，数据库名，表名
policy_json = {
    "isEnabled": True,
    "version": 1,
    "service": "csap_hive",
    "name": "testzhangch09241",
    "policyType": 0,
    "policyPriority": 0,
    "description": "Policy for test",
    "isAuditEnabled": True,
    "resources": {
        "database": {
            "values": [
                "csap"
            ],
            "isExcludes": False,
            "isRecursive": False
        },
        "column": {
            "values": [
                "*"
            ],
            "isExcludes": False,
            "isRecursive": False
        },
        "table": {
            "values": [
                "tb_dw_ct_ngcs_oc_svr_u_cntmng_day"
            ],
            "isExcludes": False,
            "isRecursive": False
        }
    },
    "policyItems": [
        {
            "accesses": [
                {
                    "type": "select",
                    "isAllowed": True
                }
            ],
            "users": [
                "zls",
                "csap991"
            ],
            "groups": [],
            "conditions": [],
            "delegateAdmin": False
        }
    ],
    "denyPolicyItems": [],
    "allowExceptions": [],
    "denyExceptions": [],
    "dataMaskPolicyItems": [],
    "rowFilterPolicyItems": [],
    "options": {},
    "validitySchedules": [],
    "policyLabels": [
        ""
    ]
}

# 变量：策略名，数据库名，表名
row_policy_json = {
    "isEnabled": True,
    "version": 1,

    # 服务名
    "service": "csap_hive",
    # 策略名
    "name": "testhyn0924222",
    # 策略类型，2行级策略，0普通策略
    "policyType": 2,
    "policyPriority": 0,
    "description": "",
    "isAuditEnabled": True,
    "resources": {
        "database": {
            "values": ["csap"],
            "isExcludes": False,
            "isRecursive": False
        },
        "table": {

            # 表名
            "values": [
                "tb_dw_ct_cti_contact_event_day"
            ],
            "isExcludes": False,
            "isRecursive": False
        }
    },
    "policyItems": [],
    "denyPolicyItems": [],
    "allowExceptions": [],
    "denyExceptions": [],
    "dataMaskPolicyItems": [],
    "rowFilterPolicyItems": [
        {
            "accesses": [
                {
                    "type": "select",
                    "isAllowed": True
                }
            ],
            "users": [
                "csap100"
            ],
            "groups": [],
            "conditions": [],
            "delegateAdmin": False,
            "rowFilterInfo": {
                "filterExpr": "prov_code='100'"
            }
        },
        {
            "accesses": [
                {
                    "type": "select",
                    "isAllowed": True
                }
            ],
            "users": [
                "csap200"
            ],
            "groups": [],
            "conditions": [],
            "delegateAdmin": False,
            "rowFilterInfo": {
                "filterExpr": "prov_code='200'"
            }
        },
        {
            "accesses": [
                {
                    "type": "select",
                    "isAllowed": True
                }
            ],
            "users": [
                "csap210"
            ],
            "groups": [],
            "conditions": [],
            "delegateAdmin": False,
            "rowFilterInfo": {
                "filterExpr": "prov_code='210'"
            }
        },
        {
            "accesses": [
                {
                    "type": "select",
                    "isAllowed": True
                }
            ],
            "users": [
                "csap220"
            ],
            "groups": [],
            "conditions": [],
            "delegateAdmin": False,
            "rowFilterInfo": {
                "filterExpr": "prov_code='220'"
            }
        },
        {
            "accesses": [
                {
                    "type": "select",
                    "isAllowed": True
                }
            ],
            "users": [
                "csap230"
            ],
            "groups": [],
            "conditions": [],
            "delegateAdmin": False,
            "rowFilterInfo": {
                "filterExpr": "prov_code='230'"
            }
        },
        {
            "accesses": [
                {
                    "type": "select",
                    "isAllowed": True
                }
            ],
            "users": [
                "csap240"
            ],
            "groups": [],
            "conditions": [],
            "delegateAdmin": False,
            "rowFilterInfo": {
                "filterExpr": "prov_code='240'"
            }
        },
        {
            "accesses": [
                {
                    "type": "select",
                    "isAllowed": True
                }
            ],
            "users": [
                "csap250"
            ],
            "groups": [],
            "conditions": [],
            "delegateAdmin": False,
            "rowFilterInfo": {
                "filterExpr": "prov_code='250'"
            }
        },
        {
            "accesses": [
                {
                    "type": "select",
                    "isAllowed": True
                }
            ],
            "users": [
                "csap270"
            ],
            "groups": [],
            "conditions": [],
            "delegateAdmin": False,
            "rowFilterInfo": {
                "filterExpr": "prov_code='270'"
            }
        },
        {
            "accesses": [
                {
                    "type": "select",
                    "isAllowed": True
                }
            ],
            "users": [
                "csap280"
            ],
            "groups": [],
            "conditions": [],
            "delegateAdmin": False,
            "rowFilterInfo": {
                "filterExpr": "prov_code='280'"
            }
        },
        {
            "accesses": [
                {
                    "type": "select",
                    "isAllowed": True
                }
            ],
            "users": [
                "csap290"
            ],
            "groups": [],
            "conditions": [],
            "delegateAdmin": False,
            "rowFilterInfo": {
                "filterExpr": "prov_code='290'"
            }
        },
        {
            "accesses": [
                {
                    "type": "select",
                    "isAllowed": True
                }
            ],
            "users": [
                "csap351"
            ],
            "groups": [],
            "conditions": [],
            "delegateAdmin": False,
            "rowFilterInfo": {
                "filterExpr": "prov_code='351'"
            }
        },
        {
            "accesses": [
                {
                    "type": "select",
                    "isAllowed": True
                }
            ],
            "users": [
                "csap311"
            ],
            "groups": [],
            "conditions": [],
            "delegateAdmin": False,
            "rowFilterInfo": {
                "filterExpr": "prov_code='311'"
            }
        },
        {
            "accesses": [
                {
                    "type": "select",
                    "isAllowed": True
                }
            ],
            "users": [
                "csap371"
            ],
            "groups": [],
            "conditions": [],
            "delegateAdmin": False,
            "rowFilterInfo": {
                "filterExpr": "prov_code='371'"
            }
        },
        {
            "accesses": [
                {
                    "type": "select",
                    "isAllowed": True
                }
            ],
            "users": [
                "csap431"
            ],
            "groups": [],
            "conditions": [],
            "delegateAdmin": False,
            "rowFilterInfo": {
                "filterExpr": "prov_code='431'"
            }
        },
        {
            "accesses": [
                {
                    "type": "select",
                    "isAllowed": True
                }
            ],
            "users": [
                "csap451"
            ],
            "groups": [],
            "conditions": [],
            "delegateAdmin": False,
            "rowFilterInfo": {
                "filterExpr": "prov_code='451'"
            }
        },
        {
            "accesses": [
                {
                    "type": "select",
                    "isAllowed": True
                }
            ],
            "users": [
                "csap471"
            ],
            "groups": [],
            "conditions": [],
            "delegateAdmin": False,
            "rowFilterInfo": {
                "filterExpr": "prov_code='471'"
            }
        },
        {
            "accesses": [
                {
                    "type": "select",
                    "isAllowed": True
                }
            ],
            "users": [
                "csap531"
            ],
            "groups": [],
            "conditions": [],
            "delegateAdmin": False,
            "rowFilterInfo": {
                "filterExpr": "prov_code='531'"
            }
        },
        {
            "accesses": [
                {
                    "type": "select",
                    "isAllowed": True
                }
            ],
            "users": [
                "csap551"
            ],
            "groups": [],
            "conditions": [],
            "delegateAdmin": False,
            "rowFilterInfo": {
                "filterExpr": "prov_code='551'"
            }
        },
        {
            "accesses": [
                {
                    "type": "select",
                    "isAllowed": True
                }
            ],
            "users": [
                "csap571"
            ],
            "groups": [],
            "conditions": [],
            "delegateAdmin": False,
            "rowFilterInfo": {
                "filterExpr": "prov_code='571'"
            }
        },
        {
            "accesses": [
                {
                    "type": "select",
                    "isAllowed": True
                }
            ],
            "users": [
                "csap591"
            ],
            "groups": [],
            "conditions": [],
            "delegateAdmin": False,
            "rowFilterInfo": {
                "filterExpr": "prov_code='591'"
            }
        },
        {
            "accesses": [
                {
                    "type": "select",
                    "isAllowed": True
                }
            ],
            "users": [
                "csap731"
            ],
            "groups": [],
            "conditions": [],
            "delegateAdmin": False,
            "rowFilterInfo": {
                "filterExpr": "prov_code='731'"
            }
        },
        {
            "accesses": [
                {
                    "type": "select",
                    "isAllowed": True
                }
            ],
            "users": [
                "csap771"
            ],
            "groups": [],
            "conditions": [],
            "delegateAdmin": False,
            "rowFilterInfo": {
                "filterExpr": "prov_code='771'"
            }
        },
        {
            "accesses": [
                {
                    "type": "select",
                    "isAllowed": True
                }
            ],
            "users": [
                "csap791"
            ],
            "groups": [],
            "conditions": [],
            "delegateAdmin": False,
            "rowFilterInfo": {
                "filterExpr": "prov_code='791'"
            }
        },
        {
            "accesses": [
                {
                    "type": "select",
                    "isAllowed": True
                }
            ],
            "users": [
                "csap851"
            ],
            "groups": [],
            "conditions": [],
            "delegateAdmin": False,
            "rowFilterInfo": {
                "filterExpr": "prov_code='851'"
            }
        },
        {
            "accesses": [
                {
                    "type": "select",
                    "isAllowed": True
                }
            ],
            "users": [
                "csap871"
            ],
            "groups": [],
            "conditions": [],
            "delegateAdmin": False,
            "rowFilterInfo": {
                "filterExpr": "prov_code='871'"
            }
        },
        {
            "accesses": [
                {
                    "type": "select",
                    "isAllowed": True
                }
            ],
            "users": [
                "csap891"
            ],
            "groups": [],
            "conditions": [],
            "delegateAdmin": False,
            "rowFilterInfo": {
                "filterExpr": "prov_code='891'"
            }
        },
        {
            "accesses": [
                {
                    "type": "select",
                    "isAllowed": True
                }
            ],
            "users": [
                "csap898"
            ],
            "groups": [],
            "conditions": [],
            "delegateAdmin": False,
            "rowFilterInfo": {
                "filterExpr": "prov_code='898'"
            }
        },
        {
            "accesses": [
                {
                    "type": "select",
                    "isAllowed": True
                }
            ],
            "users": [
                "csap931"
            ],
            "groups": [],
            "conditions": [],
            "delegateAdmin": False,
            "rowFilterInfo": {
                "filterExpr": "prov_code='931'"
            }
        },
        {
            "accesses": [
                {
                    "type": "select",
                    "isAllowed": True
                }
            ],
            "users": [
                "csap951"
            ],
            "groups": [],
            "conditions": [],
            "delegateAdmin": False,
            "rowFilterInfo": {
                "filterExpr": "prov_code='951'"
            }
        },
        {
            "accesses": [
                {
                    "type": "select",
                    "isAllowed": True
                }
            ],
            "users": [
                "csap971"
            ],
            "groups": [],
            "conditions": [],
            "delegateAdmin": False,
            "rowFilterInfo": {
                "filterExpr": "prov_code='971'"
            }
        },
        {
            "accesses": [
                {
                    "type": "select",
                    "isAllowed": True
                }
            ],
            "users": [
                "csap991"
            ],
            "groups": [],
            "conditions": [],
            "delegateAdmin": False,
            "rowFilterInfo": {
                "filterExpr": "prov_code='991'"
            }
        }
    ],
    "options": {},
    "validitySchedules": [],
    "policyLabels": [
        ""
    ]
}


def main(policy_type, database, table_name):
    # 策略类型
    if policy_type == '1':
        result_policy_json = row_policy_json
    else:
        result_policy_json = policy_json

    # 定义策略名
    now_time = str(date_time.datetime.now())[0:19].replace('-', '').replace(' ', '').replace(':', '')
    if policy_type == '1':
        result_policy_json['name'] = 'row_' + table_name + '_' + now_time
    else:
        result_policy_json['name'] = table_name + '_' + now_time

    # 库名
    result_policy_json['resources']['database']['values'] = [database]

    # 表名
    result_policy_json['resources']['table']['values'] = [table_name]

    response = requests.request("POST", url, data=json.dumps(row_policy_json), headers=headers)

    create_status = ''
    # 策略生成失败
    if response.status_code != 200:
        print '策略创建失败'
        print response.text
        print response.status_code

        create_status = '1'

    else:
        print '策略创建成功', result_policy_json['name'], result_policy_json['resources']['database']['values'], \
            result_policy_json['resources']['table']['values']

        create_status = '0'

    # 更新mysql
    ranger_sql = "insert into  tb_ranger_policy (policy_name,db_name,table_name,create_status,policy_type,create_time)  values('%s','%s','%s','%s','%s','%s')" % (
        result_policy_json['name'],database, table_name, create_status, policy_type, now_time)

    print 'ranger_sql:', ranger_sql

    conn_db.insert(ranger_sql)


# 启动
if __name__ == '__main__':

    input_length = len(sys.argv)
    print 'input_str: ', len(sys.argv)

    if input_length == 4:

        policy_type = sys.argv[1]
        database = sys.argv[2]
        table_name = sys.argv[3]
        print 'policy_type,database,table_name:', policy_type, database, table_name
        main(policy_type, database, table_name)

    else:
        print '输入参数有误'
