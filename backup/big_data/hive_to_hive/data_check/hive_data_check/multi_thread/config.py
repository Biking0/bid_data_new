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

# 连接苏研集群
excute_sy_sh = "hive -e ' use csap;"

# 生产环境
# excute_desc_sh = "beeline -u 'jdbc:hive2://192.168.190.88:10000/csap' -n hive -p %Usbr7mx -e "
excute_ocdp_sh = "beeline -u 'jdbc:hive2://hua-dlzx2-a0202:10000/csap' -n ocdp -p 1q2w1q@W -e "

# 测试环境
# excute_ocdp_test_sh = "beeline -u 'jdbc:hive2://172.22.248.19:10000/default' -n csap -p @WSX2wsx -e "
