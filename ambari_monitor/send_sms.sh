#!/usr/bin/env python
# -*-coding:utf-8 -*-
# *******************************************************************************
# 文件名称：send_sms.sh
# 功能描述：监控短信发送通用脚本，可在脚本内部调用该脚本
# 输入参数：短信内容，过滤参数
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20191010
# 修改日志：
# 修改日期：
# *******************************************************************************
# 程序调用格式：sh send_sms.sh sms_info ALL
# *******************************************************************************

# 该参数默认放短信发送内容，也可放服务器信息
sms_info=$1
# 发送用户过滤
all_info=$2

# 数据库登录命令
exec_mysql_ng="mysql -h10.97.192.180 -ungtassuite -pAj7y32h! ngtassuite"

# 发送短信，不输出报错信息
${exec_mysql_ng} -e "INSERT INTO tb_sys_sms_send_cur(serv_number, send_date,text,opt_user,opt_date ) SELECT serv_number, now(), '$sms_info', opt_user, now() FROM tb_sys_sms_phone WHERE opt_user = 'ocetl' and sms_id = '$all_info';" 2>/dev/null