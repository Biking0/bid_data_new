#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：config.py
# 功能描述：分区配置文件
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20200630
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python config.py
# ***************************************************************************


# vt
# test_database = {'csapdwd': ['csapdwd''qaQA#3e4r1'],
#                  'csapdws': ['csapdws', 'qaQA#3e4r1'],
#                  'csapdwv': ['csapdwv', 'qaQA#3e4r3'],
#                  'csapdw': ['csapdw', 'qaQA#3e4r4'],
#                  'csapdm': ['csapdm', 'qaQA#3e4r5'],
#                  'csaprp': ['csaprp', 'qaQA#3e4r6'],
#                  'csapsi': ['csapsi', 'qaQA#3e4r7'],
#                  'csapdim': ['csapdim', 'qaQA#3e4r8'],
#                  'csapetl': ['csapetl', 'qaQA#3e4r10'],
#                  # 'csapsi':'qaQA#3e4r7',
#
#                  }

test_database = {'csapdwd': ['csap', '1qaz!QAZ'],
                 'csapdws': ['csap', '1qaz!QAZ'],
                 'csapdwv': ['csap', '1qaz!QAZ'],
                 'csapdw': ['csap', 'DwCap!QAZ'],
                 'csapdm': ['csap', 'DwCap!QAZ'],
                 'csaprp': ['csap', '01#J1B(n'],
                 'csapsi': ['csap', 'SaCap!QAZ'],
                 'csapdim': ['csap', 'DmCap12345'],
                 'CSAPETL': ['csap', 'I8hjH$Sw'],
                 }

# check_table_name = 'tb_vt_data_check_test_v1'
check_table_name = 'tb_vt_data_check_v1'

# vt_ip = '172.19.74.63'
vt_ip = '192.168.190.121'

mysql_exec = "mysql -h 172.19.168.22 -P 3308 -u zhao -pzhao zhao -e \'"
